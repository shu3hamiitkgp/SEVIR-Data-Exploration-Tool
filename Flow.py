from diagrams import Cluster, Edge, Diagram
from diagrams.onprem.client import User, Users
from diagrams.onprem.container import Docker
from diagrams.onprem.workflow import Airflow
from diagrams.aws.storage import SimpleStorageServiceS3 as S3
from diagrams.onprem.network import Nginx
from diagrams.onprem.database import Postgresql as PostgreSQL 
from diagrams.oci.monitoring import Telemetry
from diagrams.gcp.analytics import Composer


with Diagram("Project 2", show=False):
    # Define Nodes
    ingress = Users("User")
    with Cluster("Compute Instance"):
        with Cluster("Google Cloud"):
            userfacing = Docker("Streamlit")
            backend = Docker("FastAPI")
            userfacing - Edge(label = "API calls", color="red", style="dashed") - backend
        
        with Cluster("Database"):
            db = PostgreSQL ("Sqlite DB")

        with Cluster("Batch Process"):
            gcc = Composer("Google Cloud Composer")
            airflow = Airflow("Airflow")
            GE = Telemetry("Data Quality Check")
            hosting = Nginx("Reports")
            dataset = S3("GOES & NEXRAD Public S3 Bucket")
            dataset_user = S3("User S3 Bucket")


    # Define Edges  


    # backend validation
    ingress >> Edge(label = "Login to Dashboard",color="darkgreen") << userfacing
    backend << Edge(label="Verify Login") << db
    devlopers = User("Developers")
    db >> Edge(label="Retrive data") >> dataset_user


    # Batch Processing
    gcc >> Edge(label="GCC Airflow connection") >> airflow
    airflow >> Edge(label="fetch data") >> dataset
    GE << Edge(label="CSV of metadata") << db
    GE >> Edge(label="Host the static html report") >> hosting
    airflow >> Edge(label="Run Great Expectation") >> GE
    airflow >> Edge(label=" Store database and CSV") >> dataset_user    
    airflow >> Edge(label="Store metadata in db") >> db

    devlopers << Edge(label = "View Reports",color="darkgreen") << hosting
    devlopers << Edge(label = "View Dashboard",color="darkgreen") << airflow
    