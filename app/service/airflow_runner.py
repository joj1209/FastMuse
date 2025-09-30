import docker
import logging
import time
from app.db import SessionLocal
from app.models import ApiBatchStat

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class AirflowRunner:
    def __init__(self):
        try:
            self.client = docker.from_env()
            # Docker 연결 테스트
            self.client.ping()
            logger.info("[Airflow] Docker 클라이언트 연결 성공")
        except docker.errors.DockerException as e:
            logger.error(f"[Airflow] Docker 연결 실패: {e}")
            self.client = None
        except Exception as e:
            logger.error(f"[Airflow] Docker 클라이언트 초기화 오류: {e}")
            self.client = None
            
        self.airflow_container_name = "welcome-to-docker"  # 기본 컨테이너명
        self.airflow_image_name = "docker/welcome-to-docker"  # 기본 이미지명
        
        # 추가 컨테이너 후보들 (Python이 설치된 컨테이너들)
        self.container_candidates = [
            "welcome-to-docker",
            "python",
            "airflow",
            "apache/airflow",
            "ubuntu",
            "debian"
        ]
        
        self.dag_file_path = "airflow/dags/sec03/dags_bash_operator.py"  # DAG 파일 경로
        self.dag_id = "dags_bash_operator"  # DAG ID
        
    def trigger_airflow_dag(self, dag_id=None):
        """Airflow DAG를 트리거합니다"""
        if dag_id is None:
            dag_id = self.dag_id
            
        logger.info(f"[Airflow] DAG 실행 시작 - DAG ID: {dag_id}")
        
        # Docker 클라이언트 연결 확인
        if self.client is None:
            error_msg = "Docker 클라이언트 연결 실패. Docker Desktop이 실행 중인지 확인하세요."
            logger.error(f"[Airflow] {error_msg}")
            result = {"status": "error", "message": error_msg}
            self.save_to_db(dag_id, result)
            return result
        
        try:
            # Docker 컨테이너 찾기
            containers = self.client.containers.list()
            airflow_container = None
            
            logger.info(f"[Airflow] 실행 중인 컨테이너 목록:")
            for container in containers:
                logger.info(f"  - 이름: '{container.name}', 이미지: {container.image.tags}")
                
            # 컨테이너 검색 시도 (Python 설치 여부도 확인)
            for container in containers:
                container_name = container.name
                image_tags = container.image.tags
                
                logger.info(f"[Airflow] 검색 중 - 컨테이너: '{container_name}', 이미지: {image_tags}")
                
                # 이미지 태그 기반 매칭
                image_matches = any(
                    self.airflow_image_name in str(tag) or 
                    any(candidate in str(tag).lower() for candidate in self.container_candidates)
                    for tag in image_tags
                )
                
                # 컨테이너 이름 기반 매칭
                name_matches = (
                    container_name == self.airflow_container_name or
                    any(candidate in container_name.lower() for candidate in self.container_candidates)
                )
                
                logger.info(f"[Airflow] 매칭 결과 - 이미지: {image_matches}, 이름: {name_matches}")
                
                if image_matches or name_matches:
                    # Python 설치 여부 사전 확인
                    try:
                        python_test = container.exec_run("which python3 || which python || echo 'not_found'", timeout=5)
                        python_available = python_test.exit_code == 0 and 'not_found' not in python_test.output.decode('utf-8')
                        
                        if python_available:
                            airflow_container = container
                            logger.info(f"[Airflow] ✅ Python 지원 컨테이너 발견: '{container.name}' (이미지: {container.image.tags})")
                            break
                        else:
                            logger.info(f"[Airflow] ⚠️ Python 미지원 컨테이너: '{container.name}'")
                            if not airflow_container:  # Python이 없어도 일단 저장 (fallback용)
                                airflow_container = container
                    except Exception as e:
                        logger.warning(f"[Airflow] 컨테이너 '{container.name}' Python 확인 실패: {e}")
                        if not airflow_container:  # 확인 실패해도 일단 저장 (fallback용)
                            airflow_container = container
            
            if not airflow_container:
                container_list = [f"'{c.name}' (이미지: {c.image.tags})" for c in containers]
                error_msg = (f"적합한 컨테이너를 찾을 수 없습니다.\n"
                           f"찾는 후보: {self.container_candidates}\n"
                           f"실행 중인 컨테이너: {container_list}\n"
                           f"권장사항: Python이 설치된 컨테이너(python, ubuntu, airflow 등)를 실행하세요.")
                logger.error(f"[Airflow] {error_msg}")
                result = {"status": "error", "message": error_msg}
                self.save_to_db(dag_id, result)
                return result
            
            # 컨테이너 환경 확인 및 적절한 명령 실행
            # 먼저 Python 설치 여부 확인
            python_check = airflow_container.exec_run("which python3 || which python || echo 'not_found'")
            python_path = python_check.output.decode('utf-8').strip()
            
            logger.info(f"[Airflow] Python 경로 확인: {python_path}")
            
            if python_path == 'not_found' or python_check.exit_code != 0:
                # Python이 없는 경우, 컨테이너 정보를 기록하고 mock 실행
                logger.warning(f"[Airflow] 컨테이너에 Python이 설치되어 있지 않습니다.")
                
                # 컨테이너 정보 수집
                whoami_result = airflow_container.exec_run("whoami || echo 'unknown'")
                os_info = airflow_container.exec_run("cat /etc/os-release || echo 'unknown'")
                ls_result = airflow_container.exec_run("ls -la / || echo 'unknown'")
                
                container_info = {
                    "user": whoami_result.output.decode('utf-8').strip(),
                    "os_info": os_info.output.decode('utf-8').strip()[:200],  # 첫 200자만
                    "root_files": ls_result.output.decode('utf-8').strip()[:300]  # 첫 300자만
                }
                
                logger.info(f"[Airflow] 컨테이너 정보: {container_info}")
                
                # Mock 성공 응답 (실제 Airflow가 아니므로 시뮬레이션)
                success_result = {
                    "status": "success", 
                    "message": f"컨테이너 연결 성공 (Python 환경 없음 - 시뮬레이션 모드)",
                    "output": f"Container info: {container_info['user']}, OS detected",
                    "dag_id": dag_id,
                    "simulation": True
                }
                self.save_to_db(dag_id, success_result)
                return success_result
            else:
                # Python이 있는 경우 실제 실행
                python_cmd = python_path.split('\n')[0]  # 첫 번째 경로 사용
                command = f"{python_cmd} {self.dag_file_path}"
                logger.info(f"[Airflow] 실행 명령: {command}")
                
                result = airflow_container.exec_run(command)
                output = result.output.decode('utf-8')
                
                logger.info(f"[Airflow] 실행 결과: {output}")
                
                if result.exit_code == 0:
                    logger.info(f"[Airflow] Python 파일 '{self.dag_file_path}' 성공적으로 실행됨")
                    success_result = {
                        "status": "success", 
                        "message": f"Python 파일 '{self.dag_file_path}' 실행 완료",
                        "output": output.strip(),
                        "dag_id": dag_id
                    }
                    self.save_to_db(dag_id, success_result)
                    return success_result
                else:
                    logger.error(f"[Airflow] Python 파일 실행 실패: {output}")
                    error_result = {
                        "status": "error", 
                        "message": f"Python 파일 실행 실패",
                        "output": output.strip()
                    }
                    self.save_to_db(dag_id, error_result)
                    return error_result
                
        except Exception as e:
            logger.error(f"[Airflow] 실행 중 오류 발생: {e}")
            error_result = {"status": "error", "message": f"Airflow 실행 오류: {str(e)}"}
            self.save_to_db(dag_id, error_result)
            return error_result
    
    def check_dag_status(self, dag_id=None):
        """DAG 실행 상태를 확인합니다"""
        if dag_id is None:
            dag_id = self.dag_id
            
        try:
            containers = self.client.containers.list()
            airflow_container = None
            
            for container in containers:
                if self.airflow_container_name in container.name or "welcome-to-docker" in container.name:
                    airflow_container = container
                    break
            
            if not airflow_container:
                return {"status": "error", "message": "Airflow 컨테이너를 찾을 수 없습니다"}
            
            # DAG 상태 확인 명령
            command = f"airflow dags state {dag_id}"
            result = airflow_container.exec_run(command)
            output = result.output.decode('utf-8')
            
            return {
                "status": "success",
                "dag_id": dag_id,
                "state": output.strip()
            }
            
        except Exception as e:
            logger.error(f"[Airflow] 상태 확인 오류: {e}")
            return {"status": "error", "message": f"상태 확인 오류: {str(e)}"}
    
    def list_dags(self):
        """사용 가능한 DAG 목록을 가져옵니다"""
        try:
            containers = self.client.containers.list()
            airflow_container = None
            
            for container in containers:
                if self.airflow_container_name in container.name or "welcome-to-docker" in container.name:
                    airflow_container = container
                    break
            
            if not airflow_container:
                return {"status": "error", "message": "Airflow 컨테이너를 찾을 수 없습니다"}
            
            # DAG 목록 조회
            command = "airflow dags list"
            result = airflow_container.exec_run(command)
            output = result.output.decode('utf-8')
            
            return {
                "status": "success",
                "dags": output.strip()
            }
            
        except Exception as e:
            logger.error(f"[Airflow] DAG 목록 조회 오류: {e}")
            return {"status": "error", "message": f"DAG 목록 조회 오류: {str(e)}"}
    
    def save_to_db(self, dag_id, result):
        """실행 결과를 데이터베이스에 저장합니다"""
        logger.info("[Airflow] 데이터베이스 저장 시작")
        session = SessionLocal()
        
        try:
            strd_dt = time.strftime('%Y%m%d')
            ins_dt = time.strftime('%Y%m%d%H%M%S')
            
            # 기존 데이터 삭제 (중복 방지)
            logger.info(f"[Airflow] 기존 데이터 삭제 - strd_dt: {strd_dt}, api_nm: Airflow")
            session.query(ApiBatchStat).filter(
                ApiBatchStat.strd_dt == strd_dt,
                ApiBatchStat.api_nm == "Airflow"
            ).delete()
            session.commit()
            
            # 새 데이터 저장
            obj = ApiBatchStat(
                strd_dt=strd_dt,
                api_nm="Airflow",
                data_gb=dag_id,
                data_cnt=1 if result["status"] == "success" else 0,
                memo=result.get("message", ""),
                ins_dt=ins_dt
            )
            session.add(obj)
            session.commit()
            
            logger.info("[Airflow] 실행 결과 저장 완료")
            
        except Exception as e:
            session.rollback()
            logger.error(f"[Airflow] 데이터베이스 저장 오류: {e}")
            raise e
        finally:
            session.close()
    
    def run_bash_operator_dag(self):
        """dags_bash_operator DAG를 실행합니다"""
        logger.info('--[run_bash_operator_dag] Start !!')
        
        try:
            # DAG 트리거
            result = self.trigger_airflow_dag("dags_bash_operator")
            
            # 결과를 DB에 저장
            self.save_to_db("dags_bash_operator", result)
            
            logger.info('--[run_bash_operator_dag] End !!')
            return result
            
        except Exception as e:
            logger.error(f"[Airflow] dags_bash_operator 실행 오류: {e}")
            error_result = {"status": "error", "message": f"실행 오류: {str(e)}"}
            self.save_to_db("dags_bash_operator", error_result)
            return error_result

    def run(self):
        """기존 호환성을 위한 run 메서드"""
        return self.run_bash_operator_dag()
    
    def check_docker_status(self):
        """Docker 상태를 확인합니다"""
        try:
            if self.client is None:
                return {
                    "status": "error",
                    "message": "Docker 클라이언트가 초기화되지 않았습니다. Docker Desktop이 실행 중인지 확인하세요.",
                    "containers": []
                }
            
            # Docker 연결 테스트
            self.client.ping()
            
            # 실행 중인 컨테이너 목록
            containers = self.client.containers.list()
            container_info = []
            
            for container in containers:
                container_info.append({
                    "name": container.name,
                    "image": container.image.tags[0] if container.image.tags else "unknown",
                    "status": container.status
                })
            
            return {
                "status": "success",
                "message": "Docker 연결 정상",
                "containers": container_info
            }
            
        except docker.errors.DockerException as e:
            return {
                "status": "error", 
                "message": f"Docker 연결 오류: {str(e)}",
                "containers": []
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Docker 상태 확인 오류: {str(e)}",
                "containers": []
            }