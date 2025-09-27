[airflow_final.md](https://github.com/user-attachments/files/22576464/airflow_final.md)
<div dir="rtl">

<div dir="rtl">

# Apache Airflow

## ۱. مقدمه

### معرفی
Apache Airflow یک پلتفرم متن‌باز (Open Source) برای **طراحی، زمان‌بندی (Scheduling)، اجرا و مانیتورینگ** جریان‌های کاری (Workflows) است.  
ایده‌ی اصلی Airflow بر پایه‌ی **Workflows as Code** شکل گرفته است؛ یعنی به‌جای ابزارهای گرافیکی، جریان کار با پایتون تعریف می‌شود و تمام منطق، وابستگی‌ها و پیکربندی‌ها در کد ثبت می‌گردد.

</div>

### تاریخچه
Airflow در سال ۲۰۱۴ توسط Airbnb توسعه یافت و در ۲۰۱۹ به **Top-Level Project** بنیاد Apache تبدیل شد. این تغییر باعث بلوغ سریع اکوسیستم و پذیرش گسترده در حوزه‌ی **مدیریت داده و اتوماسیون جریان‌های کاری** شد.

### مفاهیم کلیدی
- **DAG (Directed Acyclic Graph):** گراف بدون چرخه‌ای از Taskها با وابستگی‌های مشخص.
- **Task:** کوچک‌ترین واحد اجرایی در یک Workflow.
- **Operator:** قالب از پیش‌تعریف‌شده برای انجام یک نوع کار (بش/پایتون/SQL/API و…).
- **Scheduler:** مؤلفه‌ای که اجرای Taskها را بر اساس برنامه و وابستگی‌ها زمان‌بندی می‌کند.
- **Executor / Worker:** موتور اجرا و پردازشگرهایی که Taskها را اجرا می‌کنند.
- **Triggerer:** مؤلفه‌ای برای مدیریت Taskهای معطل (Deferrable) بدون اشغال Worker.

### ویژگی‌های کلیدی
- **مقیاس‌پذیری:** از حالت محلی تا خوشه‌های بزرگ توزیع‌شده.
- **انعطاف‌پذیری:** تعریف DAG با پایتون و توسعه‌ی Operator/Sensor/Hook سفارشی.
- **مشاهده‌پذیری (Observability):** داشبورد وب، لاگ، SLA، آمار و متریک‌ها.
- **اکوسیستم غنی:** بسته‌های ارائه‌دهنده (Providers) برای سرویس‌ها و دیتابیس‌های مختلف.
- **قابلیت استقرار متنوع:** Local، Docker، Kubernetes، و سرویس‌های مدیریت‌شده.

### کاربردهای رایج
- **ETL/ELT:** استخراج، تبدیل و بارگذاری داده‌ها.
- **Big Data & Analytics:** هماهنگ‌سازی Spark/Hadoop/Trino/Presto.
- **ML/AI:** زمان‌بندی آموزش/ارزیابی/استقرار مدل‌ها و مدیریت Data Prep.
- **DevOps/Platform:** نگه‌داری دوره‌ای، مانیتورینگ سرویس‌ها، و اتوماسیون Infra.

### شعار پروژه

<div dir="ltr">

> *Airflow is not just a scheduler; it is a platform to author, schedule and monitor workflows.*

</div>

---

## ۲. معماری Airflow

![معماری Airflow](https://airflow.apache.org/docs/apache-airflow/stable/_images/arch-diagram.png)

### مؤلفه‌ها
- **Webserver:** رابط کاربری برای مشاهده DAGها، وضعیت Taskها، لاگ‌ها و مدیریت.
- **Scheduler:** اسکن DAGها، محاسبه وابستگی‌ها، زمان‌بندی، و قرار دادن Task در صف.
- **Executor:** تصمیم‌گیرنده‌ی نحوه و محل اجرای Task (محلی، توزیع‌شده، K8s و…).
- **Workers:** پردازنده‌های واقعی Taskها.
- **Triggerer:** مدیریت Taskهای Deferrable بدون مصرف Worker.
- **Metadata Database:** نگهداری متادیتا و تاریخچه‌ی اجرا (معمولاً PostgreSQL).
- **Message Broker (در Celery):** واسط پیام بین Scheduler و Workerها (RabbitMQ/Redis).

### نمای ASCII (شمای کلی)

<div dir="ltr">

```text
                  ┌─────────────────────┐
                  │     Webserver       │
                  │     (Flask UI)      │
                  └─────────┬───────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │      Scheduler      │
                  │  DAG scan & queue   │
                  └─────────┬───────────┘
                            │              ┌───────────────────┐
                            │              │     Triggerer     │
                            │              │  deferrable tasks │
                            │              └───────────────────┘
         ┌──────────────────┼──────────────────┐
         │                  │                  │
         ▼                  ▼                  ▼
┌─────────────────┐  ┌─────────────────┐  ┌──────────────────┐
│  Metadata DB    │  │    Executor     │  │    DAG Parser    │
│ (PostgreSQL)    │  │ dispatch tasks  │  │  parse .py/.zip  │
└─────────────────┘  └─────────┬───────┘  └──────────────────┘
                               │
                               ▼
                       ┌─────────────────┐
                       │     Workers     │
                       │  run task code  │
                       └─────────────────┘
```

---

</div>


## ۳. انواع Executorها

Executor تعیین می‌کند Taskها در کجا و چگونه اجرا شوند. انتخاب Executor روی **مقیاس‌پذیری، ایزولیشن، هزینه‌ی عملیاتی و پیچیدگی نگه‌داری** اثر مستقیم دارد.

### جدول مقایسه Executorها
| Executor | معماری اجرا | ایزولیشن پردازش | مقیاس‌پذیری | پیچیدگی عملیاتی | موارد استفاده |
|---|---|---|---|---|---|
| **SequentialExecutor** | تک‌ریسمانی روی یک فرآیند | بسیار کم | بسیار محدود | بسیار کم | تست/آموزش/دمو |
| **LocalExecutor** | چندفرآیندی روی یک ماشین | متوسط | محدود به منابع همان میزبان | کم | تیم کوچک/محیط ساده |
| **CeleryExecutor** | Workerهای توزیع‌شده + Broker | متوسط | بالا (افزودن Worker) | متوسط | Production متعارف |
| **KubernetesExecutor** | هر Task در Pod مجزا | بالا | بسیار بالا (خوشه K8s) | متوسط تا زیاد | Cloud-native/ایزولیشن قوی |
| **EdgeExecutor** *(سفارشی/مفهومی)* | گره‌های مرزی/دوردست | متغیر | وابسته به توپولوژی Edge | متغیر | IoT/شبکه‌های ناپایدار |

> نکته: **EdgeExecutor** در Airflow به صورت پیش‌فرض وجود ندارد و یک طراحی/پیاده‌سازی سفارشی است.

### ۳.۱ SequentialExecutor
**تعریف:** ساده‌ترین حالت؛ در هر لحظه تنها یک Task اجرا می‌شود.  
**مزایا:** کم‌هزینه، پیکربندی ساده، مناسب آموزش و تست محلی.  
**معایب:** عدم موازی‌سازی؛ مناسب Production نیست.

### ۳.۲ LocalExecutor
**تعریف:** اجرای همزمان چند Task روی یک میزبان با **multiprocessing**.  
**مزایا:** بهره‌گیری از CPU/RAM میزبان، راه‌اندازی آسان.  
**معایب:** نقطه شکست واحد؛ مقیاس‌پذیری محدود به منابع همان سرور.

**نمونه پیکربندی (`airflow.cfg`)**

<div dir="ltr">

```ini
[core]
executor = LocalExecutor
parallelism = 32              ; حداکثر Task همزمان در کل سیستم
max_active_tasks_per_dag = 16 ; Airflow >=2.2
max_active_runs_per_dag = 1
```
</div>


**نکته‌های عملی**
- مقادیر *parallelism* و *max_active_tasks_per_dag* را با توجه به هسته‌ها و RAM تنظیم کنید.
- از **Pool** برای حفاظت از منابع محدود خارجی (اتصال DB، Rate-limit API) استفاده کنید.

### ۳.۳ CeleryExecutor
**تعریف:** معماری توزیع‌شده با **Broker** (RabbitMQ/Redis) و Workerهای متعدد. Scheduler پیام‌ها را به صف می‌فرستد؛ Workerها برداشته و اجرا می‌کنند.

**مزایا**
- مقیاس‌پذیری افقی با افزودن Worker.
- تفکیک صف‌ها (Queue) بر اساس بارهای کاری متفاوت.
- گزینه‌ی رایج Production با هزینه‌ی عملیاتی معقول.

**معایب**
- نیازمند نگه‌داری Broker/Backend.
- تنظیمات دقیق برای کارایی (Prefetch, Concurrency, Autoscaling) لازم است.

**الگوی اجزا**

<div dir="ltr">

```text
Scheduler → Broker (RabbitMQ/Redis) → Worker(s)
                       ↑
               Result Backend (DB)
```

</div>


**نمونه پیکربندی (`airflow.cfg`)**

<div dir="ltr">

```ini
[core]
executor = CeleryExecutor

[celery]
broker_url = amqp://user:pass@rabbitmq:5672//
result_backend = db+postgresql://airflow:***@postgres/airflow
worker_concurrency = 8
task_track_started = True
worker_prefetch_multiplier = 1
pool = prefork
```
</div>



**Queue‌بندی در DAG**

<div dir="ltr">


```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "queued_example",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    t_fast = BashOperator(task_id="fast", bash_command="echo fast", queue="default")
    t_gpu  = BashOperator(task_id="gpu_job", bash_command="python train.py", queue="gpu")
```
</div>


**نکته‌های عملی**
- Broker پایدار (RabbitMQ خوشه‌ای) و Backend مطمئن (PostgreSQL) برای Production.
- تنظیم **Prefetch=1** در بارهای ناهمگن برای جلوگیری از گرسنگی Taskهای کوتاه.
- مانیتورینگ طول صف، نرخ شکست/Retry، مصرف CPU/RAM Workerها.

### ۳.۴ KubernetesExecutor
**تعریف:** برای هر Task یک **Pod** مجزا می‌سازد؛ ایزولیشن قوی و مقیاس‌پذیری بسیار بالا.

**مزایا**
- ایزولیشن کانتینری (Image، منابع، Secret/Volume جدا).
- مقیاس‌پذیر مطابق ظرفیت خوشه.
- مناسب برای وابستگی‌های متفاوت و بارهای ناهمگن.

**معایب**
- راه‌اندازی و نگه‌داری K8s پیچیده‌تر.
- **Cold-start** Podها زمان شروع Task را افزایش می‌دهد.

**نمونه پیکربندی (`airflow.cfg`)**

<div dir="ltr">

```ini
[core]
executor = KubernetesExecutor

[kubernetes]
in_cluster = True
namespace = airflow
delete_worker_pods = True
delete_worker_pods_on_failure = True
multi_namespace_mode = False
worker_container_repository = my-registry/airflow-task
worker_container_tag = latest
pod_template_file = /opt/airflow/pod_template.yaml
```

</div>


**نمونه `pod_template.yaml` (خلاصه)**

<div dir="ltr">


```yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: airflow
spec:
  serviceAccountName: airflow-worker
  containers:
    - name: base
      image: my-registry/airflow-task:latest
      imagePullPolicy: IfNotPresent
      resources:
        requests:
          cpu: "500m"
          memory: "1Gi"
        limits:
          cpu: "2"
          memory: "4Gi"
      env:
        - name: AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION
          value: "True"
      volumeMounts:
        - name: work
          mountPath: /opt/airflow
  volumes:
    - name: work
      emptyDir: {}
```

</div>


**نکته‌های عملی**
- تعیین دقیق `resources.requests/limits` برای Taskهای سنگین.
- استفاده از NodeSelector/Toleration برای Jobهای GPU یا حساس.
- فعال‌سازی پاک‌سازی Podهای قدیمی: `delete_worker_pods* = True`.

### ۳.۵ EdgeExecutor *(سفارشی)*
**تعریف:** اجرای Taskها در گره‌های مرزی نزدیک منبع داده (کارخانه، شعب دوردست) برای **تاخیر کم** و **تاب‌آوری در قطعی شبکه**.

**ویژگی‌های طراحی**
- نیازمند الگوی ارتباطی سازگار با محدودیت Edge (Push/Pull/Queueing محلی).
- **Idempotency** و قابلیت Resume برای Taskها.
- فشرده‌سازی و انتقال امن Log/Artifact به مرکز.

**نمونه پیکربندی مفهومی**

<div dir="ltr">

```ini
[core]
executor = EdgeExecutor  ; پیاده‌سازی سفارشی

[edge]
discovery_url = https://edge-gateway.internal
auth_token = ${EDGE_TOKEN}
offline_buffer_dir = /var/lib/airflow-edge-buffer
```
</div>


### راهنمای تصمیم‌گیری سریع
- **تست/آموزش:** SequentialExecutor
- **یک سرور قدرتمند بدون کلاستر:** LocalExecutor
- **Production مرسوم با رشد تدریجی:** CeleryExecutor
- **Cloud-native با وابستگی‌های متنوع:** KubernetesExecutor
- **Edge/IoT با اتصال ناپایدار:** پیاده‌سازی سفارشی EdgeExecutor

---

## ۴. Scheduler (زمان‌بند)

**وظایف اصلی**
- اسکن مسیرهای DAG و Parse کدها.
- ارزیابی وابستگی‌ها، `start_date`، `schedule` و **Catchup**.
- ایجاد **DagRun** و صف‌گذاری **TaskInstance**ها.
- رسیدگی به Retry/Reschedule، SLA و Failover.

**پارامترهای مهم (`airflow.cfg`)**

<div dir="ltr">

```ini
[scheduler]
scheduler_heartbeat_sec = 5
min_file_process_interval = 30
max_tis_per_query = 512
max_active_runs_per_dag = 1
max_active_tasks_per_dag = 16
parse_sqlalchemy_pool_size = 5
```
</div>

**نکته‌ها**
- برای DAGهای زیاد، `min_file_process_interval` را افزایش دهید تا فشار Parse کاهش یابد.
- از **DAG serialization** (وب‌سرور بی‌نیاز از دسترسی مستقیم به کد) در محیط‌های توزیع‌شده استفاده کنید.
- **Catchup** را برای DAGهایی که به گذشته اهمیتی ندارند غیرفعال کنید.

**نمونه DAG با تنظیمات زمان‌بندی**
<div dir="ltr">

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_task():
    print("run")

with DAG(
    dag_id="example_scheduler",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,  # سطح DAG
) as dag:
    PythonOperator(task_id="t", python_callable=run_task)
```

---
</div>


## ۵. Webserver (رابط کاربری)

**قابلیت‌ها**
- مشاهده DAGها، گراف وابستگی، Gantt، Tree، لاگ‌ها و SLA.
- کنترل دستی: Trigger، Clear، Pause/Unpause، Mark Success/Failed.
- RBAC (نقش‌محور) و احراز هویت (OAuth/LDAP/… از طریق Flask AppBuilder).

**پیکربندی (`airflow.cfg`)**

<div dir="ltr">

```ini
[webserver]
web_server_host = 0.0.0.0
web_server_port = 8080
workers = 4
worker_refresh_interval = 30
expose_config = False
rbac = True
```
</div>


**نکات امنیتی**
- پشت Reverse Proxy (TLS) قرار دهید.
- کاربران و نقش‌ها را با **Flask AppBuilder** مدیریت کنید.
- `expose_config=False` برای جلوگیری از افشای تنظیمات حساس.

**راه‌اندازی**
<div dir="ltr">

```bash
airflow webserver
airflow users create --role Admin --username admin --password ***** --firstname A --lastname B --email admin@example.com
```

---
</div>


## ۶. Database (PostgreSQL)

**وظایف**
- نگهداری متادیتا: DAG, DagRun, TaskInstance, XCom, Logs, SLA, Job, Slot, Connection, Variable.
- تاریخچه اجرای Taskها و وضعیت‌ها.

**پیشنهاد Production**
- PostgreSQL مدیریت‌شده یا HA (نمونه: Patroni + etcd).
- جداسازی Storage از Compute، پشتیبان‌گیری منظم.

**تنظیم اتصال**
<div dir="ltr">

```ini
[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:***@postgres/airflow
sql_engine_encoding = utf-8
load_default_connections = False
```
</div>


**نگه‌داری**
<div dir="ltr">

```bash
airflow db migrate          # مهاجرت/به‌روزرسانی Schema
airflow db check            # بررسی سلامت DB
airflow db cleanup --dry-run --days 30  # پاکسازی رکوردهای قدیمی
```
</div>


**بهینه‌سازی**
- ایندکس روی ستون‌های پرکاربرد (dag_id, task_id, execution_date/state).
- تنظیمات Pool/Pool Size برای SQLAlchemy.
- مانیتورینگ قفل‌ها و زمان اجرای Queryها.

---

## ۷. RabbitMQ & Queue (Celery)

**نقش در معماری**
- واسط پیام بین Scheduler و Workerها (Producer/Consumer).
- پایداری و Throughput بالا برای محیط‌های Production.

**High Availability**
- **Clustering + Mirrored Queues**، **Quorum Queues** در نسخه‌های جدید.
- راه‌اندازی Auto-failover و مانیتورینگ Health.

**پیکربندی نمونه RabbitMQ (docker-compose خلاصه)**

<div dir="ltr">

```yaml
version: "3.9"
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports: ["15672:15672", "5672:5672"]
    environment:
      RABBITMQ_DEFAULT_USER: airflow
      RABBITMQ_DEFAULT_PASS: strongpass
    volumes:
      - rabbitmq-data:/var/lib/rabbitmq
volumes:
  rabbitmq-data: {}
```
</div>


**پارامترهای Celery مهم**

<div dir="ltr">

```ini
[celery]
broker_url = amqp://airflow:strongpass@rabbitmq:5672//
result_backend = db+postgresql://airflow:***@postgres/airflow
worker_concurrency = 8
worker_prefetch_multiplier = 1   ; جلوگیری از over-fetch
task_acks_late = True            ; قابلیت Retry امن
```
</div>


**نکات عملی**
- برای Jobهای ناهمگن، Queueهای مجزا تعریف کنید (GPU، IO-bound، CPU-bound).
- Auto-scaling Worker بر اساس طول صف و مصرف منابع.
- نظارت بر Dead-letter و پیام‌های معلق.

---

## ۸. Sensors

**کارکرد:** انتظار برای وقوع شرایط خارجی (فایل، زمان، HTTP، SQL و…).

### الگوهای اجرا
- **Poke Mode:** سنجش دوره‌ای؛ ساده اما مصرف‌کننده‌ی Slot Worker.
- **Reschedule Mode:** آزاد کردن Slot بین سنجش‌ها.
- **Deferrable Sensors:** انتقال انتظار به Triggerer؛ کارآمدترین روش در Airflow 2.x.

### نمونه‌ها

<div dir="ltr">

**FileSensor (reschedule)**
```python
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def example_filesensor():
    wait_for_file = FileSensor(
        task_id="wait_for_input",
        filepath="/data/input.txt",
        poke_interval=60,
        mode="reschedule",
        timeout=6*60*60
    )
example_filesensor()
```

**HttpSensor (deferrable)**
```python
from airflow.decorators import dag
from airflow.sensors.http import HttpSensorAsync  # deferrable variant
from datetime import datetime

@dag(schedule=None, start_date=datetime(2024,1,1), catchup=False)
def example_http_deferrable():
    HttpSensorAsync(
        task_id="wait_api_ok",
        http_conn_id="my_api",
        endpoint="/health",
        response_check=lambda r: r.status_code == 200,
        poke_interval=30,
        timeout=3600
    )
example_http_deferrable()
```

**SqlSensor**
```python
from airflow.sensors.sql import SqlSensor
SqlSensor(
    task_id="wait_until_rows_exist",
    conn_id="warehouse",
    sql="SELECT 1 FROM events WHERE processed = false LIMIT 1;",
    poke_interval=60,
    mode="reschedule"
)
```
</div>


**نکته‌ها**
- برای انتظارهای طولانی از **Deferrable** استفاده کنید تا ظرفیت Worker آزاد بماند.
- حتماً `timeout` تعریف کنید تا DAG بی‌نهایت معطل نماند.

---

## ۹. XCom (Cross-Communication)

**تعریف:** مکانیزم تبادل داده‌های کوچک بین Taskها (ذخیره در Metadata DB).  
**محدودیت اندازه:** حدود ۴۸ کیلوبایت برای هر XCom. برای داده‌های بزرگ از Storageهای خارجی استفاده کنید (S3/GCS/DB).

<div dir="ltr">

### TaskFlow API و XComArg

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2024,1,1), schedule=None, catchup=False)
def example_xcom():
    @task
    def extract():
        return {"msg": "hello", "n": 42}

    @task
    def transform(payload: dict) -> str:
        return f"{payload['msg']} #{payload['n']}"

    @task
    def load(text: str):
        print(text)

    load(transform(extract()))
example_xcom()
```
</div>


### Backend سفارشی برای XCom
می‌توانید با پیاده‌سازی **XCom Backend**، مقادیر بزرگ را در Storage خارجی نگهداری کرده و در DB فقط مرجع ذخیره شود.

**نمونه پیکربندی**

<div dir="ltr">

```ini
[core]
xcom_backend = my_project.xcom.S3XComBackend
```

---
</div>



## ۱۰. نتیجه‌گیری

Airflow بستری توانمند برای **طراحی، زمان‌بندی و پایش** Workflowهاست. انتخاب درست Executor، تنظیم دقیق Scheduler، استفاده‌ی هوشمند از Sensors و مدیریت مناسب پایگاه داده/صف پیام، کلید ساخت یک سامانه‌ی **مقیاس‌پذیر، قابل‌اتکا و قابل‌مشاهده** است. مسیر رشد معمول تیم‌ها از **Sequential → Local → Celery → Kubernetes** است و در سناریوهای خاص می‌توان سراغ طراحی‌های Edge رفت.

**چک‌لیست راه‌اندازی**
- انتخاب Executor متناسب با بار کاری.
- تنظیم `airflow.cfg` و مدیریت Secrets.
- تعریف Queues/Pools یا Pod Template (در K8s).
- سیاست‌های Retry/SLA و زمان‌بندی Catchup.
- مانیتورینگ و آلارم‌ها (صف‌ها، Workerها، DB).
- راهبرد پاک‌سازی و پشتیبان‌گیری منظم.


</div>
