from app.service.youtube_comment_crawler import YoutubeCommentCrawler
import logging

logging.basicConfig(level=logging.INFO)

def test_youtube_crawler():
    print("=== YouTube 댓글 크롤러 테스트 ===")
    
    crawler = YoutubeCommentCrawler()
    print(f"API 키: {crawler.api_key[:20]}...")
    
    # 1. 동영상 검색 테스트
    print("\n1. 동영상 검색 테스트")
    video_ids = crawler.video_search_list('노트북', 3)
    print(f"검색된 비디오 ID들: {video_ids}")
    
    if not video_ids:
        print("동영상 검색 실패!")
        return
    
    # 2. 동영상 정보 가져오기 테스트
    print("\n2. 동영상 정보 테스트")
    video_list = crawler.get_video_info(video_ids)
    print(f"동영상 수: {len(video_list)}")
    
    for i, video in enumerate(video_list):
        title = video["title"][:50] + "..." if len(video["title"]) > 50 else video["title"]
        print(f"{i+1}. {title} (댓글: {video['commentCount']})")
    
    # 3. 댓글 가져오기 테스트
    print("\n3. 댓글 가져오기 테스트")
    if video_list:
        first_video = video_list[0]
        print(f"테스트 동영상: {first_video['video_id']}")
        
        try:
            comments = crawler.get_comments('노트북', first_video['video_id'], 5)
            print(f"수집된 댓글 수: {len(comments)}")
            
            for i, comment in enumerate(comments[:3]):
                author = comment["comment_author"]
                text = comment["main_text"][:50] + "..." if len(comment["main_text"]) > 50 else comment["main_text"]
                print(f"  {i+1}. {author}: {text}")
                
        except Exception as e:
            print(f"댓글 수집 오류: {str(e)}")
    
    # 4. 전체 크롤링 테스트
    print("\n4. 전체 크롤링 테스트")
    try:
        all_comments = crawler.crawl_comments_by_keyword('노트북', 2)
        print(f"전체 수집된 댓글 수: {len(all_comments)}")
        
        if all_comments:
            print("샘플 댓글:")
            for i, comment in enumerate(all_comments[:3]):
                author = comment["comment_author"]
                text = comment["main_text"][:30] + "..." if len(comment["main_text"]) > 30 else comment["main_text"]
                print(f"  {i+1}. {author}: {text}")
    except Exception as e:
        print(f"전체 크롤링 오류: {str(e)}")

if __name__ == "__main__":
    test_youtube_crawler()