import docker
import logging
import time
from app.db import SessionLocal
from app.models import ApiBatchStat

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class AirflowRunner:
    def __init__(self):
        self.client = docker.from_env()
        self.airflow_container_name = "airflow-scheduler"  # 실제 컨테이너명에 맞게 수정
        self.dag_id = "dags_bash_operator"  # DAG ID
        
    def trigger_airflow_dag(self, dag_id=None):
        """Airflow DAG를 트리거합니다"""
        if dag_id is None:
            dag_id = self.dag_id
            
        logger.info(f"[Airflow] DAG 실행 시작 - DAG ID: {dag_id}")
        
        try:
            # Airflow 컨테이너 찾기
            containers = self.client.containers.list()
            airflow_container = None
            
            for container in containers:
                if self.airflow_container_name in container.name or "airflow" in container.name.lower():
                    airflow_container = container
                    logger.info(f"[Airflow] 컨테이너 발견: {container.name}")
                    break
            
            if not airflow_container:
                logger.error("[Airflow] Airflow 컨테이너를 찾을 수 없습니다")
                return {"status": "error", "message": "Airflow 컨테이너를 찾을 수 없습니다"}
            
            # DAG 트리거 명령 실행
            command = f"airflow dags trigger {dag_id}"
            logger.info(f"[Airflow] 실행 명령: {command}")
            
            result = airflow_container.exec_run(command)
            output = result.output.decode('utf-8')
            
            logger.info(f"[Airflow] 실행 결과: {output}")
            
            if result.exit_code == 0:
                logger.info(f"[Airflow] DAG '{dag_id}' 성공적으로 트리거됨")
                return {
                    "status": "success", 
                    "message": f"DAG '{dag_id}' 실행이 트리거되었습니다",
                    "output": output.strip()
                }
            else:
                logger.error(f"[Airflow] DAG 트리거 실패: {output}")
                return {
                    "status": "error", 
                    "message": f"DAG 트리거 실패: {output.strip()}"
                }
                
        except Exception as e:
            logger.error(f"[Airflow] 실행 중 오류 발생: {e}")
            return {"status": "error", "message": f"Airflow 실행 오류: {str(e)}"}
    
    def check_dag_status(self, dag_id=None):
        """DAG 실행 상태를 확인합니다"""
        if dag_id is None:
            dag_id = self.dag_id
            
        try:
            containers = self.client.containers.list()
            airflow_container = None
            
            for container in containers:
                if self.airflow_container_name in container.name or "airflow" in container.name.lower():
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
                if self.airflow_container_name in container.name or "airflow" in container.name.lower():
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