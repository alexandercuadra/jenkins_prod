from elasticsearch import Elasticsearch 
import re
import os
from git import Repo, Git
import glob
import json
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

#Protocolo de ELK
logger = getLogger(__name__):
def _index_doc(doc):
    elastic_config = config["elastic"]
    elastic_config = config["elastic"]
    url = f'{elastic_config["protocol"]}://{elastic_config["host"]}:{elastic_config["port"]}'
    logger.debug(f'url {url}')
    logger.debug(f'Inserting doc {doc}')
    es =  Elasticsearch(
            [url],
            timeout=5000,
            http_auth=(elastic_config["user"], elastic_config["pass"])
        )
    
    result = es.index(index = elastic_config["index"],body = doc)
    logger.debug(f'result {result}').

    retur result
    
def test


    

#Función para publicar los parametros de Jenkins a ElasticSearch
def publish(request):
    logger.debug("publishing...")
    build = {"author": dict(request.author), "approver": dict(request.approver),
         "build": dict(request.build), "application": dict(request.application),
         "pull_request": dict(request.pull_request), "to_prod":  request.pull_request.dst_branch == "master"}
    logger.debug("build doc...")
    timestamp = re.sub('(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2})(\d{2})', r'\1:\2',  str(request.timestamp))
    doc = {"@timestamp":timestamp, "platform": "jenkins", "module": "build", "jenkins": {"build": build }}
    logger.debug(f'doc: {doc}')
    result = _index_doc(doc)
    return (codes["OK"], result["result"])

#Función para clonar y hacer pull al repositorio
def _clone_repo():
    logger.debug(f'Clone repo- Getting config')
    bitbucket_config = config["bitbucket"]
    logger.debug(f'Clone repo- Bitbucket config {bitbucket_config}')
    url = f'{bitbucket_config["url"]}/{bitbucket_config["project"]}/{bitbucket_config["repository"]}'
    directory = os.path.join(os.getcwd(),f'{bitbucket_config["repository"]}')
    if os.path.exists(directory):
        logger.info(f'Clone repo-Directory exists,  pulling the project...')
        repo = Repo(directory)

        logger.debug(f'Clone repo-Remote name: {repo.remotes.origin.name}')
        logger.debug(f'Clone repo-Remote URL: {repo.remotes.origin.url}')
        logger.debug(f'Clone repo-Pulling')
        with repo.git.custom_environment(GIT_SSH_COMMAND=f'ssh -v -i {bitbucket_config["path_rsa"]} -o "StrictHostKeyChecking=no"'):
            repo.remotes.origin.pull()
    else:
        logger.info("Clone repo-Directory doesn´t exist, cloning repository....")
        Repo.clone_from(url, directory, branch=f'{bitbucket_config["src_branch"]}',
            env={"GIT_SSH_COMMAND": f'ssh  -i {bitbucket_config["path_rsa"]} -o "StrictHostKeyChecking=no"'})

#concatenate files
def _get_services_from_file(file): 
    try: 
        with open(file) as config_file:
            try: 
                if file.endswith(".json"):
                    config = json.load(config_file)
                elif file.endswith(".yml"):
                    config = yaml.load(config_file, Loader=Loader)
                else: 
                    config = {}
                if "services" in config: 
                    return config["services"]
                else: 
                    logger.warning(f'File {file} has not services')
                    return []
            except Exception as e: 
                    logger.warning(f'Error proccessing file  {file} ')
                    return []
    except Exception as e: 
                    logger.warning(f'{file} not exists ')
                    return []
     

def _get_services(conf_dir):
    services = []
    files = [] 
    if os.path.isdir(conf_dir):
        conf_dir = f'{conf_dir}/' if conf_dir[-1] != "/" else conf_dir
        for file in os.listdir(conf_dir): 
            if file.endswith(".json") or  file.endswith(".yml"):
                services += [(service, f'{conf_dir}{file}')   for service in _get_services_from_file(f'{conf_dir}{file}')] 
    return services

def get_data(request):
    data = {"service": dict(request.service), "environment": request.environment,
         "file": request.file, "connector": dict(request.connector)}
    logger.debug(f'Resultado de: {codes["OK"]}')
    logger.debug(f'Checking data: {data}')
    return (codes["OK"], data)

#add services
def _add_new_service(services, data):
    service = {"service": data.service.name,
                    "domain": data.service.domain,
                    "owner": {"name": data.service.owner.name,"mail": data.service.owner.mail},
                    "severity": 7 if data.environment == "prod" else 5
        }       
    services.append(service)  
    return services              

#Funcion para crear el conector
def monitoring_connector(data):
    logger.debug(f'Cloning repo')
    services = _clone_repo()
    logger.debug(f'Getting services')
    repository = config["bitbucket"]["repository"]
    services = _get_services(f'{repository}/conf.d')
    found_service = False 
    found_connector = False
    logger.debug(f'Looking for service')
    for service in services:
        if data.service.name in service[0]["service"]:
            found_service = True 
            service_file = service[1]
            break       
    if (found_service):
        logger.debug(f'Services found in file {service_file}')
        services =   _get_services_from_file(service_file) 
        
    else: 
        logger.debug(f'Service Not found')
        services =  _get_services_from_file(data.file) 
        services =  _add_new_service(services, data)
        service_file = data.file
    logger.debug(f'Creating API')
    connector_monitoring =  {
                                "technology": "api",
                                "name": f'{data.connector.name}',    
                                "hosts": [f'prod-connect-nbib-{data.environment}.be.ocjc.serv.dc.es.telefonica'], 
                                "protocol": "http",
                                "endpoint": f'/connectors/{data.connector.name}/status', 
                                "method": "GET",
                                "checks": []
    }
            
    new_services = []
    logger.debug(f'Adding/Replacing API')
    for service in services: 
            if service["service"] == data.service.name:
                        if "platforms" not in service: 
                            service["platforms"] = {}
                        if "rest" not in service["platforms"]:
                            service["platforms"]["rest"] = [{"apis": []}]
                        rest_list = []
                        for rest in service["platforms"]["rest"]:
                            if "apis" not in rest:
                                rest["apis"] = []
                            apis = [] 
                            for api in rest["apis"]:
                                if api["name"] != data.connector.name:
                                    apis.append(api)
                            task_list = []
                            for i in range(data.connector.number_of_tasks):
                                task_state = 'tasks.{}.state'.format(i)
                                task = {"type": "field", "args": {"field": task_state,"op": "eq", "const": "RUNNING" }}
                                task_list.append(task)
                            connector_monitoring["checks"] = task_list
                            apis.append(connector_monitoring) 
                            rest["apis"]=apis 
                            rest_list.append(rest)
                        service["platforms"]["rest"] = rest_list
            new_services.append(service)
    logger.debug(f'Editing file {service_file} with {new_services}')
    with open(service_file, 'w') as f:
        f.write(json.dumps({"services": new_services}, indent=2))
    _commit_and_push("maruja-config")
    return (codes["OK"], new_services) 

#Funcion para eliminar conectore y apis
def del_monitoring(data):
    try:
        logger.debug(f'Cloning repo')
        services = _clone_repo()
        logger.debug(f'Getting services')
        services = _get_services("maruja-config/conf.d")
        found_service = False 
        found_connector = False
        for service in services:
            if data.service in service[0]["service"]:
                found_service = True 
                service_file = service[1]
                break       
        if (found_service):
            logger.debug(f'Services found in file {service_file}')
            services =   _get_services_from_file(service_file) 
        else:
            logger.warning(f'Service doesn´t exist {data}')

        #borra servicio        
        updated_services = []
        services[:] = [service for service in services if not service["service"] == data.service]
        logger.debug(f'Deleting Service {data.service}')
        logger.debug(services)

        #borra conector
        for service in services:
            if service["service"] == data.service:
                for rest in service["platforms"]["rest"]:
                    apis_list = rest["apis"]
                    apis_list[:] = [api for api in apis_list if not api["name"] == data.connector]
                    logger.debug(f'Deleting Connector {data.connector} ')
        for service in services: 
            if service["service"] == data.service:
                        if "platforms" not in service: 
                            service["platforms"] = {}
                        if "rest" not in service["platforms"]:
                            service["platforms"]["rest"] = [{"apis": []}]
                        rest_list = []
                        for rest in service["platforms"]["rest"]:
                            if "apis" not in rest:
                                rest["apis"] = []
                            apis = [] 
                            for api in rest["apis"]:
                                if api["name"] == data.connector:
                                    apis.append(api)
                            rest_list.append(rest)   
                        service["platforms"]["rest"] = rest_list           
            updated_services.append(service)
        logger.debug(f'Editing file {service_file} with {updated_services}')  
        with open(service_file, 'w') as f:  
            f.write(json.dumps({"services": updated_services}, indent=2))
        _delete_commit_and_push("maruja-config")
        return (codes["OK"], updated_services)
    except Exception as e:
        logger.error(e) 
        
#Función que realiza commit y push de nuevos servicios/conectores
def _commit_and_push(directory):
    bitbucket_config = config["bitbucket"]
    repo = Repo(directory)
    directory = os.path.join(os.getcwd(),f'{bitbucket_config["repository"]}')
    repo.git.add(all=True)
    logger.debug("Adding changes to branch")
    repo.index.commit(message = "Adding connector in file confluent.json")
    logger.debug("Commit changes")
    with repo.git.custom_environment(GIT_SSH_COMMAND=f'ssh -v -i {bitbucket_config["path_rsa"]} -o "StrictHostKeyChecking=no"'):
        repo.git.push('--set-upstream','origin')
    logger.debug("Pushing changes to branch")

#Función que realiza commit y push de servicios/conectores eliminados
def _delete_commit_and_push(directory):  
    bitbucket_config = config["bitbucket"]
    repo = Repo(directory)
    directory = os.path.join(os.getcwd(),f'{bitbucket_config["repository"]}')
    repo.git.add(all=True)
    logger.debug("Adding changes to branch")
    repo.index.commit(message = "Deleting connector in file")
    logger.debug("Commit changes")
    with repo.git.custom_environment(GIT_SSH_COMMAND=f'ssh -v -i {bitbucket_config["path_rsa"]} -o "StrictHostKeyChecking=no"'):
        repo.git.push('--set-upstream', 'origin')
    logger.debug("Pushing changes")