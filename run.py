import importlib
import sys
import pdb

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python run.py <모듈_경로>.<함수_이름> [인자1] [인자2] ...")
        print("예시: python run.py app.service.jeju_floating_population.run_api_jeju_floating_population")
        sys.exit(1)

    full_path = sys.argv[1]
    parts = full_path.rsplit('.', 1)
    module_path, function_name = parts[0], parts[1]
    args = sys.argv[2:] # 함수에 전달할 인자들

    try:
        # 모듈을 동적으로 import
        module = importlib.import_module(module_path)
        # 모듈에서 함수를 가져옴
        func = getattr(module, function_name)

        # 함수를 실행하기 직전에 디버거를 시작
        pdb.set_trace()
        
        # 함수 실행
        func(*args)

    except (ImportError, AttributeError) as e:
        print(f"에러: '{module_path}' 모듈에서 '{function_name}' 함수를 찾거나 실행할 수 없습니다.")
        print(e)
        sys.exit(1)
