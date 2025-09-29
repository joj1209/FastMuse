from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from ..db import get_db
from ..models import YoutubeComment  # YoutubeKeword → YoutubeComment로 변경
from ..service.youtube_comment_crawler import YoutubeCommentCrawler
from pydantic import BaseModel
import time

class YoutubeCommentRequest(BaseModel):
    keyword: str
    max_results: int = 10

router = APIRouter(prefix="/collect", tags=["collect"])

@router.post("/youtube_comments")
async def collect_youtube_comments(
    request: YoutubeCommentRequest, 
    bg: BackgroundTasks, 
    db: Session = Depends(get_db)
):
    """YouTube 댓글 수집 API"""
    try:
        # YoutubeCommentCrawler를 사용해서 댓글 수집
        crawler = YoutubeCommentCrawler()
        
        # run_crawl_google_youtube_comment 메서드 사용
        crawler.run_crawl_google_youtube_comment(request.keyword)
        
        # 수집된 댓글 개수 확인
        strd_dt = time.strftime('%Y%m%d')
        comment_count = db.query(YoutubeComment).filter(
            YoutubeComment.strd_dt == strd_dt,
            YoutubeComment.keword == request.keyword
        ).count()
        
        return {
            "message": f"YouTube 댓글 {comment_count}개를 수집했습니다.",
            "keyword": request.keyword,
            "total_collected": comment_count
        }
        
    except Exception as e:
        return {"error": f"댓글 수집 중 오류 발생: {str(e)}"}
