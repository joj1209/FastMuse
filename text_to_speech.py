from gtts import gTTS

text = """
안녕하세요. 이번 오디오에서는, service.file 패키지가 어떤 구조로 되어 있고, 각 클래스가 어떤 역할을 하는지 정리해서 설명드리겠습니다.

먼저 전체 구조부터 보겠습니다.

service.file 패키지는,
SQL 파일들을 한 번에 읽어서,
각 SQL이 사용하고 있는 소스 테이블과 타겟 테이블을 분석한 뒤,
텍스트 파일과 CSV 파일로 결과를 정리해서 출력하는, 작은 배치 파이프라인입니다.

Spring Batch의 Job, Step, Reader, Processor, Writer 구조를 직접 구현한 버전이라고 보시면 됩니다.

크게 두 가지 실행 모드가 있습니다.

첫 번째는 파일 단위 모드입니다.
AppJob 이라는 클래스가 담당합니다.
디렉터리 안의 모든 SQL 파일을 돌면서,
파일마다 소스 테이블, 타겟 테이블을 뽑아서,
텍스트 파일과 summary.csv를 만들어 줍니다.

두 번째는 스텝 단위 모드입니다.
AppStepJob 이라는 클래스가 담당합니다.
하나의 SQL 안에 STEP 구분 주석이 있는 경우,
예를 들어, 슬래시 스타 STEP영영일 스타 슬래시, 또는 더블 대시 STEP영영일 같은 주석을 기준으로,
SQL을 여러 STEP으로 나눈 다음,
각 STEP 안에서 사용되는 소스 테이블과 타겟 테이블을 별도로 분석해서,
스텝별 결과를 텍스트로 출력합니다.

이제 각 구성요소를 조금 더 자세히 설명하겠습니다.

먼저 AppJob 입니다.

AppJob 은 일반적인 파일 단위 파이프라인을 대표하는 Job 클래스입니다.

필드에는,
입력 디렉터리 경로,
SqlReader,
FileParserProcessor,
TextWriter,
CsvWriter가 들어 있습니다.

createDefault 메서드를 보면,
기본 입력 디렉터리와 출력 디렉터리 경로를 잡고,
SqlReader를 기본 문자셋으로 생성한 다음,
FileParserProcessor.withDefaults 를 호출해서 TableParser를 내장한 Processor를 만들고,
TextWriter와 CsvWriter를 UTF dash 8로 생성해서 AppJob에 주입합니다.

실제로 일을 하는 메서드는 stepRead입니다.

stepRead는,
SqlReader에게 inputDir을 넘기고,
각 파일마다 handleFile 이라는 콜백을 호출해 달라고 요청합니다.
모든 파일이 처리된 후에는,
CsvWriter에 쌓아 둔 레코드들을 summary.csv로 한 번에 write 해서 저장합니다.

handleFile 메서드 안에서는,
먼저 stepParse 로 SQL 문자열을 파싱해서 TablesInfo를 얻고,
그 다음 stepWrite 로 TextWriter를 통해 텍스트 파일을 만들고,
마지막으로 CsvWriter.addRecord 로 summary.csv에 한 줄을 추가하는 구조입니다.

파일 이름은, 입력 디렉터리를 기준으로 상대 경로를 구한 뒤,
확장자 .sql 을 언더스코어 sql 언더스코어 tables 점 텍스트로 바꿔서 사용합니다.

메인 메서드는 간단합니다.
createDefault로 Job을 만들고,
stepRead를 호출해서 전체를 한 번에 실행합니다.

다음은 AppStepJob 입니다.

AppStepJob 은 STEP 단위 파이프라인을 담당합니다.
입력 경로가 디렉터리일 수도 있고, 하나의 파일일 수도 있습니다.

createJob 정적 메서드를 보면,
기본 입력 경로와 출력 경로를 사용하거나,
파라미터로 받은 경로를 사용해서,
SqlReader, FileStepParserProcessor, TextStepWriter를 생성하고,
AppStepJob에 넣어 줍니다.

execute 메서드는,
입력 경로가 디렉터리인지, 파일인지에 따라 동작이 조금 다릅니다.

디렉터리인 경우에는,
SqlReader.run을 호출해서, 디렉터리 안의 모든 SQL 파일에 대해 processFile을 호출합니다.

단일 파일인 경우에는,
processSingleFile에서 readFile로 내용을 읽고,
processFile로 넘겨서 처리합니다.

processFile 메서드에서는,
먼저 parse로 SQL을 STEP 단위로 파싱해서,
STEP 이름을 키로 하고, TablesInfo를 값으로 하는 맵을 얻습니다.
그 다음 write 메서드를 통해,
TextStepWriter로 파일 하나당 언더스코어 step 언더스코어 tables 점 텍스트 파일을 생성합니다.

메인 메서드에서는,
인자가 있으면 그 인자를 입력 경로로 쓰고,
없으면 기본 경로를 사용해서 execute를 호출합니다.

다음은 SqlReader입니다.

SqlReader는 SQL 파일을 읽는 Reader 역할을 합니다.
생성자에서 문자셋을 받는데,
기본값은 UTF dash 8입니다.

readFile 메서드에서는,
Files.readAllBytes로 바이너리를 읽고,
CharsetDecoder를 사용해서 문자열로 변환합니다.
이 때 깨지는 문자나 매핑할 수 없는 문자가 있어도,
REPLACE 모드로 동작하기 때문에,
예외를 던지지 않고 특수 문자로 치환해서 읽습니다.

run 메서드는,
디렉터리 경로와 콜백 핸들러를 받아서,
Files.walk로 하위 디렉터리까지 돌면서,
확장자가 .sql 인 파일만 골라서,
각 파일에 대해 handler.handle, 즉 콜백을 호출해 줍니다.

만약 SQL 파일 인코딩이 EUC dash KR 이라면,
new SqlReader, 괄호 안에 Charset.forName, "EUC dash KR" 을 넣어서 생성하면 됩니다.

다음은 Processor 계층입니다.

FileParserProcessor는 한 SQL 문자열을 받아서 TablesInfo로 변환하는 역할만 합니다.
실제 파싱 로직은 모두 TableParser라는 클래스 안에 들어 있습니다.
withDefaults 메서드에서 new TableParser로 기본 파서를 하나 만들어서 사용합니다.

FileStepParserProcessor는 STEP 단위 파서를 위해 존재합니다.
내부에 TableStepParser를 가지고 있고,
parse 메서드에서 extractTablesByStep을 호출해서,
STEP 이름과 TablesInfo의 맵을 돌려줍니다.

이제 핵심인 TableParser를 설명하겠습니다.

TableParser는 SQL 한 덩어리를 입력으로 받아서,
TablesInfo, 즉 소스 테이블과 타겟 테이블 목록을 뽑아내는 역할을 합니다.

extractTables 메서드의 처리 순서는 다음과 같습니다.

첫째, removeComments로 SQL에서 주석을 모두 제거합니다.
블록 주석인 슬래시 스타 스타 슬래시와,
라인 주석인 더블 대시 이후 부분을 제거합니다.

둘째, WITH 절 안에 정의된 CTE, 즉 Common Table Expression의 별칭을 추출합니다.
WITH alias AS 괄호 열고 형태, 또 콤마로 이어지는 alias2 AS 괄호 열고 형태를 정규식으로 찾습니다.
이 별칭들은 실제 물리 테이블이 아니라,
WITH 내부의 이름일 뿐이기 때문에,
나중에 소스 테이블 목록에서 제외하기 위해 따로 Set으로 모아 둡니다.

셋째, 타겟 테이블을 추출합니다.
INSERT INTO, UPDATE, DELETE FROM, DELETE, MERGE INTO 같은 키워드를 기준으로,
TableTargetPattern과 TableNamePattern에서 만든 정규식을 사용해서,
키워드 뒤에 나오는 테이블 이름을 찾습니다.
찾아낸 테이블 이름은 TablesInfo의 targets에 담습니다.

넷째, 소스 테이블을 추출합니다.
FROM, LEFT JOIN, INNER JOIN, RIGHT JOIN, 일반 JOIN, USING, WITH 등,
TableSourcePattern에 정의된 여러 키워드를 사용해서 정규식을 돌립니다.
또 Oracle 스타일의, FROM 뒤에 여러 테이블을 콤마로 나열하는 문법도 지원합니다.
이 때 문자열 리터럴 안에 있는 FROM 키워드는 무시하기 위해,
isInsideSingleQuotes 라는 메서드로 따로 필터링을 합니다.

다섯째, WITH C T E 별칭을 소스 테이블 목록에서 제거합니다.
앞에서 모아 둔 CTE 별칭 Set을 sources에서 제거해서,
실제 테이블만 남도록 합니다.

TableParser는 이 모든 과정을 거쳐,
최종적으로 TablesInfo 객체를 반환합니다.

TableNamePattern이라는 보조 클래스도 중요합니다.

이 클래스에는 SQL 키워드 목록과,
테이블명 정규식, 그리고 테이블명을 정리하고 검증하는 유틸리티가 들어 있습니다.

cleanTableName 메서드는,
테이블명 뒤에 붙는 쉼표, 세미콜론, 괄호, 줄바꿈 같은 것들을 잘라내고,
불필요한 백틱 같은 것도 제거합니다.

isValidTableName 메서드는,
대문자 키워드 목록과 비교해서,
SELECT, FROM 같은 키워드는 걸러내고,
길이가 1자인 이름도 필터링해서,
실제 테이블명으로 보이는 것만 통과시키는 역할을 합니다.

TableSourcePattern과 TableTargetPattern은,
각각 소스 테이블과 타겟 테이블 추출에 필요한 키워드와 정규식을 모아 놓은 클래스입니다.

TableSourcePattern에는,
FROM, LEFT OUTER JOIN, INNER JOIN, RIGHT JOIN, JOIN, USING, WITH, 그리고 FROM 절 범위를 찾는 패턴이 정의되어 있습니다.

TableTargetPattern에는,
INSERT INTO, UPDATE, DELETE FROM, DELETE, MERGE INTO 패턴들이 정의되어 있고,
ALL_TARGET_PATTERNS 배열에 이들을 모아서,
TableParser가 공통 로직으로 돌릴 수 있게 해 둔 구조입니다.

이제 STEP 단위 파서인 TableStepParser를 보겠습니다.

TableStepParser는 전달받은 SQL 전체 문자열에서,
먼저 STEP 구분 주석을 찾습니다.

정규식은 다음 두 형태를 모두 지원합니다.
슬래시 스타 STEP 숫자 스타 슬래시,
그리고 더블 대시 STEP 숫자 입니다.

예를 들어,
슬래시 스타 STEP 영영일 스타 슬래시,
또는, 대시 대시 STEP 영영이 같은 주석을 찾습니다.

각 STEP 구간의 시작 위치와 끝 위치를 기록해 두고,
인접한 STEP 사이를 잘라서 STEP 하나에 해당하는 SQL 덩어리를 만듭니다.

그 후, 각 STEP의 SQL 덩어리에 대해,
기본 TableParser.extractTables를 호출해서,
TablesInfo를 얻습니다.

마지막으로, STEP 이름과 TablesInfo를 LinkedHashMap에 담아서 반환합니다.
이 맵은 AppStepJob에서 TextStepWriter로 넘겨집니다.

다음은 데이터를 담는 VO 클래스인 TablesInfo입니다.

TablesInfo는 두 개의 세트를 갖습니다.
sources와 targets입니다.
둘 다 LinkedHashSet을 사용해서,
추출된 순서를 유지합니다.

addSource와 addTarget 메서드로 이름을 추가할 수 있고,
isEmpty 메서드로 비어 있는지 확인합니다.
또, 정렬해서 반환하는 getSortedSources, getSortedTargets 같은 메서드도 있어서,
Writer 쪽에서 출력할 때 사용합니다.

마지막으로 Writer 계층입니다.

TextWriter는 파일 단위로 테이블 정보를 텍스트로 출력합니다.
생성자에서 출력 디렉터리와 문자셋을 받습니다.
기본값은 UTF dash 8입니다.

write 메서드는,
상대 경로를 받아서 outputDir 아래에 디렉터리를 만들고,
내용을 기록합니다.

writeTables 메서드는,
TablesInfo를 받아서,
먼저 브래킷 Source Tables 블록 안에 소스 테이블 목록을 쓰고,
그 다음 브래킷 Target Tables 블록에 타겟 테이블 목록을 쓴 후,
write 메서드로 파일을 저장합니다.

TextStepWriter는 STEP 단위 결과를 텍스트로 출력하는 역할을 합니다.

writeStepTables 메서드에서는,
입력 디렉터리를 기준으로 상대 경로를 계산해서,
원래 파일명이 sample.sql 이라면,
sample 언더스코어 step 언더스코어 tables 점 텍스트처럼 이름을 만들고,
각 STEP마다 구분선과 STEP 이름,
그리고 소스 테이블, 타겟 테이블을 순서대로 출력합니다.

CsvWriter는 summary.csv를 만드는 클래스입니다.

기본 헤더는,
File Name, Source Tables, Target Tables 세 개입니다.

addRecord 메서드에서는,
파일 이름과 TablesInfo를 받아서,
소스 테이블 목록을 하나의 문자열로 합치고,
타겟 테이블 목록도 하나의 문자열로 합쳐서,
레코드 리스트에 추가합니다.

write 메서드에서는,
우선 디렉터리를 만들고,
CSV 파일이 UTF dash 8일 경우,
엑셀에서 한글이 깨지지 않도록,
BOM, 즉 바이트 오더 마크를 앞에 써 줍니다.

그 다음 헤더를 한 줄 쓰고,
각 레코드를 차례대로 CSV 형식으로 출력합니다.

정리하면,

service.file 패키지는,

SqlReader로 파일을 읽고,
TableParser, TableStepParser가 SQL에서 소스 테이블과 타겟 테이블을 분석하고,
TextWriter, TextStepWriter, CsvWriter가 결과를 파일로 적절하게 출력하는 구조입니다.

AppJob은 파일 단위 결과와 summary.csv를 만드는 Job이고,
AppStepJob은 하나의 SQL 안에 있는 여러 STEP의 테이블 사용 현황을 분석해서,
스텝별 텍스트 결과를 만드는 Job입니다.

이 구조를 활용하면,
추후 Spring Batch로 마이그레이션 할 때,
SqlReader는 ItemReader에,
FileParserProcessor는 ItemProcessor에,
TextWriter와 CsvWriter는 ItemWriter에 매핑해서 사용할 수 있습니다.

또한 인코딩을 EUC dash KR로 바꾸고 싶다면,
SqlReader와 Writer들을 생성할 때,
Charset.forName 괄호 EUC dash KR 괄호를 넘겨서 쉽게 변경할 수 있습니다.

이상으로, service.file 패키지 구조와 역할에 대한 설명을 마칩니다.

"""

# 한국어 설정
# gTTS는 Google TTS를 사용하므로 한국어는 여성 목소리만 지원됩니다
# 남성 목소리를 원하시면 Microsoft Azure TTS나 Amazon Polly 같은 유료 서비스가 필요합니다
# 
# 속도 조절 옵션:
# - slow=True: 느린 속도
# - slow=False: 일반 속도 (기본값)
# 
# 더 빠른 속도를 원하시면 MP3 플레이어에서 1.25x ~ 1.5x 재생 속도로 들으시는 것을 권장합니다

tts = gTTS(text=text, lang='ko', slow=False)

# mp3 파일로 저장
tts.save("service_file_package_overview_ko.mp3")

print("생성 완료: service_file_package_overview_ko.mp3")
print("참고: gTTS는 한국어 여성 목소리만 지원합니다.")
print("더 빠른 속도로 들으려면 MP3 플레이어에서 재생 속도를 1.25배 이상으로 설정하세요.")
