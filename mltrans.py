import os
from mlflow import MlflowClient
import mlflow
from datetime import datetime

class module:
    def __init__(self, mlflow_port, image_port, project_type, section, eqid, chamber, recipe):
        # client
        self.uri = "http://10.9.205.78:{}/".format(mlflow_port)
        self.image_port = image_port
        self.client = MlflowClient(tracking_uri=self.uri)
        mlflow.set_tracking_uri(self.uri)
        # experient name
        self.experiment_name = section + "__" + project_type
        
        # run name
        alias = os.getlogin().lower()
        self.run_name = datetime.now().strftime("%Y%m%d %H:%M:%S") + "__" + eqid + "__" + chamber + "__" + recipe + "__" + alias
        
        # create experiment
        self.__create_experiment()
        
        # create run and get run_id
        self.run = self.__create_run()
        
        # get experiment_id
        self.experiment_id = self.__get_experiment_id()
        
        
    def __check_experiment_existence(self):
        all_experiments = self.client.search_experiments()
        for experiment in all_experiments:
            if experiment.name == self.experiment_name:
                return True
        return False

    def __create_experiment(self):
        if not self.__check_experiment_existence():
            self.client.create_experiment(self.experiment_name)
    
    def __get_experiment_id(self):
        all_experiments = self.client.search_experiments()
        for experiment in all_experiments:
            if experiment.name == self.experiment_name:
                return experiment.experiment_id

    def __create_run(self):
        experiment = self.client.get_experiment_by_name(self.experiment_name)
        experiment_id = experiment.experiment_id
        run = self.client.create_run(experiment_id)
        self.client.set_tag(run.info.run_id, "mlflow.runName", self.run_name)
        self.client.set_tag(run.info.run_id, "checked", "Wait")
        return run

    def log_params(self):
        raise NotImplementedError("Subclasses should implement this method.")

    def log_metrics(self):
        raise NotImplementedError("Subclasses should implement this method.")

    def log_artifacts(self):
        raise NotImplementedError("Subclasses should implement this method.")




# uri = "http://10.9.205.78:7777/"
# client = MlflowClient(tracking_uri=uri)
# mlflow.set_tracking_uri(uri)
# all_experiments = client.search_experiments()