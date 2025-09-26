import httpx
from bs4 import BeautifulSoup

async def fetch_example(keyword: str) -> list[dict]:
    url = f"https://example.com/search?q={keyword}"
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url)
        r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    # 실제 페이지 구조에 맞게 파싱
    results = []
    for a in soup.select("a.result"):
        results.append({
            "title": a.get_text(strip=True),
            "url": a.get("href"),
            "raw": a.parent.get_text(" ", strip=True)
        })
    return results
