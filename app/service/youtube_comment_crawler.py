import pandas as pd
import time
import logging
from googleapiclient.discovery import build
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from app.config import settings
from app.models import YoutubeComment

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class YoutubeCommentCrawler:
    
    def __init__(self, api_key=None):
        # YouTube API 키 설정 - config에서 가져오기
        self.api_key = api_key or settings.DEVELOPER_KEY
        if self.api_key == "AIzaSyAH00WKaO2C6g7QSY8Chy4tYuU4SAswyc4":
            logger.warning("YouTube API 키가 기본값입니다. config.py에서 DEVELOPER_KEY를 설정해주세요.")
        
        self.youtube_api = build("youtube", "v3", developerKey=self.api_key)
        
        # 데이터베이스 연결 설정
        engine = create_engine(settings.DB_URL)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        self.db = SessionLocal()

    def video_search_list(self, query, max_results=10):
        """키워드로 동영상 검색"""
        try:
            search_response = self.youtube_api.search().list(
                q=query,
                part='id,snippet',
                maxResults=max_results
            ).execute()

            video_ids = []
            for item in search_response.get("items", []):
                if item["id"]["kind"] == "youtube#video":
                    video_ids.append(item["id"]["videoId"])
            return ",".join(video_ids)
        except Exception as e:
            logger.error(f"동영상 검색 중 오류: {str(e)}")
            return ""

    def get_video_info(self, video_ids):
        """동영상 정보 가져오기"""
        try:
            videos_list_response = self.youtube_api.videos().list(
                id=video_ids,
                part='snippet,statistics'
            ).execute()

            video_list = []
            for item in videos_list_response.get("items", []):
                video_info = {
                    "video_id": item['id'], 
                    "title": item["snippet"]["title"],
                    "channelTitle": item["snippet"]["channelTitle"],
                    "commentCount": item["statistics"].get("commentCount", "0")
                }
                video_list.append(video_info)
            return video_list
        except Exception as e:
            logger.error(f"동영상 정보 가져오기 중 오류: {str(e)}")
            return []

    def get_comments(self, keyword, video_id, max_cnt=10):
        """특정 동영상의 댓글 가져오기"""
        try:
            comment_list_response = self.youtube_api.commentThreads().list(
                videoId=video_id,
                part='id,replies,snippet',
                maxResults=max_cnt
            ).execute()

            strd_dt = time.strftime('%Y%m%d')
            ins_dt = time.strftime('%Y%m%d%H%M%S')

            comments = []
            for comment in comment_list_response.get("items", []):
                snippet = comment['snippet']['topLevelComment']['snippet']
                
                comment_data = {
                    "strd_dt": strd_dt, 
                    "keword": keyword, 
                    "link": f"https://www.youtube.com/watch?v={snippet['videoId']}", 
                    "video_id": snippet["videoId"],
                    "main_text": snippet['textOriginal'][:100],  # 100자로 제한
                    "comment_author": snippet['authorDisplayName'], 
                    "ins_dt": ins_dt
                }
                comments.append(comment_data)
                
            return comments
        except Exception as e:
            logger.error(f"댓글 가져오기 중 오류: {str(e)}")
            return []

    def crawl_comments_by_keyword(self, keyword, video_cnt=5):
        """키워드로 동영상 검색 후 댓글 수집"""
        logger.info(f'유튜브 댓글 크롤링 시작 - 키워드: {keyword}')
        
        video_ids = self.video_search_list(keyword, video_cnt)
        if not video_ids:
            logger.warning("검색된 동영상이 없습니다.")
            return []
            
        video_list = self.get_video_info(video_ids)
        all_comments = []
        
        for video in video_list:
            try:
                comment_count = int(video['commentCount'])
                # 댓글 수 제한 (최대 100개)
                max_comments = min(comment_count, 100) if comment_count > 0 else 10
                
                comment_list = self.get_comments(keyword, video['video_id'], max_comments)
                all_comments.extend(comment_list)
                
                logger.info(f"동영상 '{video['title']}'에서 {len(comment_list)}개 댓글 수집")
            except Exception as e:
                logger.error(f"동영상 {video['video_id']} 댓글 수집 중 오류: {str(e)}")
                continue
        
        return all_comments

    def save_to_db(self, comments_data):
        """댓글 데이터를 데이터베이스에 저장"""
        try:
            saved_count = 0
            for comment_data in comments_data:
                youtube_comment = YoutubeComment(
                    strd_dt=comment_data['strd_dt'],
                    keword=comment_data['keword'],
                    link=comment_data['link'],
                    video_id=comment_data['video_id'],
                    main_text=comment_data['main_text'],
                    comment_author=comment_data['comment_author'],
                    ins_dt=comment_data['ins_dt']
                )
                self.db.add(youtube_comment)
                saved_count += 1
            
            self.db.commit()
            logger.info(f"데이터베이스에 {saved_count}개 댓글 저장 완료")
            return saved_count
        except Exception as e:
            self.db.rollback()
            logger.error(f"데이터베이스 저장 중 오류: {str(e)}")
            raise e

    def run(self, search_keyword="노트북", video_count=10):
        """유튜브 댓글 크롤링 실행"""
        try:
            logger.info(f'유튜브 댓글 크롤링 시작 - 키워드: {search_keyword}')
            
            # 댓글 데이터 수집
            comments_data = self.crawl_comments_by_keyword(search_keyword, video_count)
            
            if not comments_data:
                logger.warning("수집된 댓글이 없습니다.")
                return []
            
            # 데이터베이스에 저장
            saved_count = self.save_to_db(comments_data)
            
            logger.info(f'유튜브 댓글 크롤링 완료 - {saved_count}개 댓글 저장')
            return comments_data
            
        except Exception as e:
            logger.error(f'유튜브 댓글 크롤링 중 오류 발생: {str(e)}')
            raise e
        finally:
            self.db.close()