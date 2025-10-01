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
            
        self.airflow_container_name = "bskim-airflow-webserver-1"  # 실제 Airflow 웹서버 컨테이너
        self.airflow_image_name = "apache/airflow"  # 실제 Airflow 이미지
        
        # Airflow 컨테이너 후보들 (우선순위 순)
        self.airflow_container_candidates = [
            "bskim-airflow-webserver-1",    # 웹서버 (CLI 명령 실행 가능)
            "bskim-airflow-scheduler-1",    # 스케줄러 (CLI 명령 실행 가능)
            "bskim-airflow-worker-1",       # 워커
            "bskim-airflow-triggerer-1"     # 트리거러
        ]
        
        # 일반 컨테이너 후보들 (Airflow가 없을 때 fallback)
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
        """Airflow CLI를 사용하여 DAG를 트리거합니다"""
        if dag_id is None:
            dag_id = self.dag_id
            
        logger.info(f"[Airflow] Airflow CLI DAG 실행 시작 - DAG ID: {dag_id}")
        
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
                
            # Airflow 컨테이너 검색 (Airflow CLI가 있는 컨테이너만 사용)
            for candidate in self.airflow_container_candidates:
                for container in containers:
                    if container.name == candidate:
                        try:
                            # Airflow CLI 사용 가능 여부 확인
                            airflow_test = container.exec_run("airflow version", timeout=10)
                            if airflow_test.exit_code == 0:
                                airflow_container = container
                                logger.info(f"[Airflow] ✅ Airflow CLI 컨테이너 발견: '{container.name}'")
                                logger.info(f"[Airflow] Airflow 버전: {airflow_test.output.decode('utf-8').strip()}")
                                break
                            else:
                                logger.info(f"[Airflow] ⚠️ Airflow CLI 미지원: '{container.name}'")
                        except Exception as e:
                            logger.warning(f"[Airflow] Airflow CLI 확인 실패 '{container.name}': {e}")
                
                if airflow_container:
                    break
            
            if not airflow_container:
                container_list = [f"'{c.name}' (이미지: {c.image.tags})" for c in containers]
                error_msg = (f"Airflow CLI가 설치된 컨테이너를 찾을 수 없습니다.\n"
                           f"Airflow 컨테이너 후보: {self.airflow_container_candidates}\n"
                           f"실행 중인 컨테이너: {container_list}\n"
                           f"권장사항: apache/airflow 이미지로 Airflow 컨테이너를 실행하세요.")
                logger.error(f"[Airflow] {error_msg}")
                result = {"status": "error", "message": error_msg}
                self.save_to_db(dag_id, result)
                return result
            
            # Airflow CLI로 DAG 실행
            logger.info(f"[Airflow] Airflow CLI 컨테이너 사용: {airflow_container.name}")
            
            # 1. DAG 목록 확인
            dag_list_cmd = "airflow dags list"
            dag_list_result = airflow_container.exec_run(dag_list_cmd, timeout=30)
            logger.info(f"[Airflow] 사용 가능한 DAG 목록:\n{dag_list_result.output.decode('utf-8')}")
            
            # 2. DAG 존재 여부 확인
            dag_info_cmd = f"airflow dags show {dag_id}"
            dag_info_result = airflow_container.exec_run(dag_info_cmd, timeout=15)
            dag_exists = dag_info_result.exit_code == 0
            
            if not dag_exists:
                logger.warning(f"[Airflow] DAG '{dag_id}'가 존재하지 않습니다. 사용 가능한 예시 DAG를 실행합니다.")
                # 예시 DAG 목록에서 bash_operator 관련 찾기
                available_dags = dag_list_result.output.decode('utf-8')
                if "example_bash_operator" in available_dags:
                    dag_id = "example_bash_operator"
                    logger.info(f"[Airflow] 예시 DAG 'example_bash_operator'를 실행합니다.")
                else:
                    # 첫 번째 사용 가능한 DAG 사용
                    dag_lines = [line.strip() for line in available_dags.split('\n') if line.strip() and not line.startswith('DAGS')]
                    if dag_lines:
                        dag_id = dag_lines[0].split()[0]  # 첫 번째 컬럼이 DAG ID
                        logger.info(f"[Airflow] 첫 번째 사용 가능한 DAG '{dag_id}'를 실행합니다.")
                    else:
                        error_msg = f"실행 가능한 DAG가 없습니다. DAG 파일을 /opt/airflow/dags/ 디렉토리에 배치하세요."
                        logger.error(f"[Airflow] {error_msg}")
                        result = {"status": "error", "message": error_msg}
                        self.save_to_db(dag_id, result)
                        return result
            
            # 3. DAG 트리거 실행
            trigger_cmd = f"airflow dags trigger {dag_id}"
            logger.info(f"[Airflow] CLI 실행 명령: {trigger_cmd}")
            
            trigger_result = airflow_container.exec_run(trigger_cmd, timeout=60)
            output = trigger_result.output.decode('utf-8')
            
            logger.info(f"[Airflow] DAG 트리거 결과 (exit_code: {trigger_result.exit_code}): {output}")
            
            if trigger_result.exit_code == 0:
                # 4. DAG 실행 상태 확인 (선택적)
                status_cmd = f"airflow dags state {dag_id} $(date +%Y-%m-%d)"
                status_result = airflow_container.exec_run(status_cmd, timeout=30)
                status_output = status_result.output.decode('utf-8') if status_result.exit_code == 0 else "상태 확인 실패"
                
                success_result = {
                    "status": "success", 
                    "message": f"Airflow CLI DAG '{dag_id}' 트리거 성공",
                    "trigger_output": output.strip(),
                    "dag_status": status_output.strip(),
                    "dag_id": dag_id,
                    "container": airflow_container.name,
                    "execution_mode": "airflow_cli"
                }
                self.save_to_db(dag_id, success_result)
                return success_result
            else:
                error_result = {
                    "status": "error", 
                    "message": f"Airflow CLI DAG '{dag_id}' 트리거 실패",
                    "output": output.strip(),
                    "exit_code": trigger_result.exit_code,
                    "container": airflow_container.name
                }
                self.save_to_db(dag_id, error_result)
                return error_result
                
        except Exception as e:
            logger.error(f"[Airflow] CLI 실행 중 오류 발생: {e}")
            error_result = {"status": "error", "message": f"Airflow CLI 실행 오류: {str(e)}"}
            self.save_to_db(dag_id, error_result)
            return error_result
    
    def check_dag_status(self, dag_id=None):
        """DAG 실행 상태를 확인합니다"""
        if dag_id is None:
            dag_id = self.dag_id
            
        try:
            containers = self.client.containers.list()
            airflow_container = None
            
            # Airflow 컨테이너 우선 검색
            for candidate in self.airflow_container_candidates:
                for container in containers:
                    if container.name == candidate:
                        airflow_container = container
                        break
                if airflow_container:
                    break
            
            # 일반 컨테이너 검색 (fallback)
            if not airflow_container:
                for container in containers:
                    if any(candidate in container.name.lower() for candidate in self.container_candidates):
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
                "state": output.strip(),
                "container": airflow_container.name
            }
            
        except Exception as e:
            logger.error(f"[Airflow] 상태 확인 오류: {e}")
            return {"status": "error", "message": f"상태 확인 오류: {str(e)}"}
    
    def list_dags(self):
        """사용 가능한 DAG 목록을 가져옵니다"""
        try:
            containers = self.client.containers.list()
            airflow_container = None
            
            # Airflow 컨테이너 우선 검색
            for candidate in self.airflow_container_candidates:
                for container in containers:
                    if container.name == candidate:
                        airflow_container = container
                        break
                if airflow_container:
                    break
            
            # 일반 컨테이너 검색 (fallback)
            if not airflow_container:
                for container in containers:
                    if any(candidate in container.name.lower() for candidate in self.container_candidates):
                        airflow_container = container
                        break
            
            if not airflow_container:
                return {"status": "error", "message": "Airflow 컨테이너를 찾을 수 없습니다"}
            
            # DAG 목록 조회
            command = "airflow dags list"
            result = airflow_container.exec_run(command)
            output = result.output.decode('utf-8')
            
            # ...existing code...
            return {
                "status": "success",
                "dags": output.strip(),
                "container": airflow_container.name
            }
            
        except Exception as e:
            logger.error(f"[Airflow] DAG 목록 조회 오류: {e}")
            return {"status": "error", "message": f"DAG 목록 조회 오류: {str(e)}"}
        
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
            airflow_containers = []
            
            for container in containers:
                info = {
                    "name": container.name,
                    "image": container.image.tags[0] if container.image.tags else "unknown",
                    "status": container.status
                }
                container_info.append(info)
                # Airflow 컨테이너 식별
                if container.name in self.airflow_container_candidates or "airflow" in container.name.lower():
                    airflow_containers.append(info)
            
            return {
                "status": "success",
                "message": "Docker 연결 정상",
                "containers": container_info,
                "airflow_containers": airflow_containers,
                "airflow_candidates": self.airflow_container_candidates
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
    
    def save_to_db(self, dag_id, result):
        logger.info("[Airflow] 데이터베이스 저장 시작")
        session = SessionLocal()
        try:
            strd_dt = time.strftime('%Y%m%d')
            ins_dt = time.strftime('%Y%m%d%H%M%S')
            # 메시지를 500자로 제한 (데이터베이스 컬럼 크기 제한)
            message = result.get("message", "")
            if len(message) > 500:
                message = message[:497] + "..."
                logger.info(f"[Airflow] 메시지가 500자를 초과하여 자름: {len(result.get('message', ''))}")
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
                memo=message,  # 제한된 길이의 메시지 사용
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