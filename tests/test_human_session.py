import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest

from channel_sync.bridge import Bridge, ObservationComplete
from channel_sync.bridge_store import BridgeStore, SCOPE
from channel_sync.redaction import Redactor
from channel_sync.signatures import echo_signature
from helpers import config
from test_bridge import StubClient


class HumanSessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.conf = config(self.root, dry_run=False, telegram_to_bale_enabled=True, bale_to_telegram_enabled=True)
        self.store = BridgeStore(self.conf.database_path, Redactor())
        self.addCleanup(self.store.close)
        self.control = self.root / 'control.json'
        self.clients = {p: StubClient(p,self.conf) for p in ('telegram','bale')}
        self.bridge = Bridge(self.conf,self.clients,self.store,stop_on_error=True,watch_duplicates=True,session_control=self.control)
        self.bridge.bot_ids = {'telegram':123456789,'bale':987654321}
        self.message = {'message_id':1,'chat':{'id':self.conf.bale_channel_id,'type':'channel'},
                        'from':{'id':0,'is_bot':False},'text':'unique test marker'}

    def expect(self):
        self.store.prepare_forward_send(send_key='expected',platform='bale',chat_id=self.conf.bale_channel_id,
                                        method='sendMessage',request={},signatures=[echo_signature(self.message,'bale')],ttl=600)
        self.store.claim_forward_send('expected',600)
        self.store.finish_forward_send('expected',status='sent',response={'ok':True,'result':{'message_id':90}})

    def ingest(self, message, update_id):
        self.store.ingest('bale',SCOPE,[{'update_id':update_id,'message':message}],encoder=self.bridge.codec.encode)
        self.bridge.process_inbox('bale')

    async def test_first_anonymous_echo_ignored_extra_echo_stops_before_queue(self):
        self.expect()
        self.ingest(self.message,1)
        self.assertEqual(self.bridge.report['events'][-1]['reason'],'echo signature')
        with self.assertRaisesRegex(RuntimeError,'possible duplicate or loop'):
            self.ingest({**self.message,'message_id':2},2)
        self.assertTrue(self.store.paused('bale_to_tg'))
        self.assertEqual(self.store.snapshot()['job_counts'],{})

    async def test_api_redelivery_of_same_message_is_not_extra_echo(self):
        self.expect()
        self.ingest(self.message,1)
        self.ingest(self.message,2)
        self.assertEqual(self.bridge.report['events'][-1]['reason'],'duplicate message')
        self.assertFalse(self.store.paused('bale_to_tg'))

    async def test_human_post_goes_through_normal_inbox_and_album_buffer(self):
        self.ingest(self.message,1)
        self.assertEqual(self.bridge.report['events'][-1]['reason'],'queued')
        first={**self.message,'message_id':2,'text':'album first','media_group_id':'human-group'}
        self.ingest(first,2)
        second={**first,'message_id':3,'text':'album second'}
        self.ingest(second,3)
        rows=self.store.connection.execute("SELECT due_at,created_at FROM bridge_jobs WHERE job_key LIKE '%:album:%'").fetchall()
        self.assertEqual(len(rows),1)
        self.assertGreaterEqual(rows[0]['due_at']-rows[0]['created_at'],self.conf.album_wait)

    async def test_observation_remains_unarmed_until_user_finishes(self):
        self.control.write_text(json.dumps({'finish_requested':False,'observation_seconds':1}))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.bridge.supervise(),0.01)
        self.assertEqual(self.bridge.report['events'],[])

    async def test_finished_observation_stops_cleanly(self):
        self.bridge.last_source_at=time.time()-3
        self.control.write_text(json.dumps({'finish_requested':True,'finish_requested_at':time.time()-3,'observation_seconds':1}))
        with self.assertRaises(ObservationComplete):
            await self.bridge.supervise()
        self.assertEqual(self.bridge.report['events'][-1]['kind'],'observation_complete')
        self.assertEqual(self.bridge.report['errors'],[])

    async def test_new_human_post_extends_final_observation(self):
        self.control.write_text(json.dumps({'finish_requested':True,'finish_requested_at':time.time()-3,'observation_seconds':1}))
        self.ingest(self.message,1)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.bridge.supervise(),0.01)
        self.assertEqual(self.bridge.report['events'][-1]['reason'],'queued')
