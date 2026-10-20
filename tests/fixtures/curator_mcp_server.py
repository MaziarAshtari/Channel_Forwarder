"""Local official-SDK smoke fixture; no external network requests."""

from mcp.server.fastmcp import FastMCP

server = FastMCP("curator-test")


@server.tool(annotations={"readOnlyHint": True})
def search(query: str) -> str:
    return "https://official.example/release\nPrimary evidence for " + query


if __name__ == "__main__":
    server.run(transport="stdio")
