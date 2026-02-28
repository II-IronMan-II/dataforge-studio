import uvicorn

BANNER = """
╔══════════════════════════════════════════╗
║        DataForge Studio  v0.1.0          ║
╠══════════════════════════════════════════╣
║  Endpoints:                              ║
║    GET  /health                          ║
║    *    /api/projects                    ║
║    *    /api/tables                      ║
║    *    /api/execute                     ║
║    *    /api/compile                     ║
║    *    /api/llm                         ║
╚══════════════════════════════════════════╝
"""

if __name__ == "__main__":
    print(BANNER)
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
