import mltrans
import os
from PIL import Image

class MLCustomOutputs(mltrans.module):
    def __init__(self, mlflow_port, image_port, project_type, section, eqid, chamber, recipe):
        super().__init__(mlflow_port, image_port, project_type, section, eqid, chamber, recipe)

    def log_params(self):
        for key, value in self.params.items():
            self.client.log_param(self.run.info.run_id, key, value)
    
    def log_metrics(self):
        for key, value in self.metrics.items():
            self.client.log_metric(self.run.info.run_id, key, value)
    
    def log_artifacts(self):
        for artifact_path in self.artifacts:
            self.client.log_artifact(self.run.info.run_id, artifact_path)
        # show items
        note_content = ""
        for key, artifact_path in self.artifacts_show.items():
            self.client.log_artifact(self.run.info.run_id, artifact_path)
            file_name = artifact_path.split("\\")[-1]
            
            image = Image.open(artifact_path)
            new_size = (600, 400)
            resized_image = image.resize(new_size)
            resized_image.save(artifact_path[:-4] + "_resized.png")
            
            self.client.log_artifact(self.run.info.run_id, artifact_path[:-4] + "_resized.png")

            file_path = f"http://10.9.205.78:{self.image_port}/images/{self.experiment_id}/{self.run.info.run_id}/artifacts/{file_name[:-4]}" + "_resized.png"
            note_content += f"<h1>{key}</h1>![{key}]({file_path})<br>"
        self.client.set_tag(self.run.info.run_id, "mlflow.note.content", note_content)

    def record(self, params, metrics, artifacts, artifacts_show):
        self.params = params  # {}
        self.metrics = metrics  # {}
        self.artifacts = artifacts  # []
        self.artifacts_show = artifacts_show  # {}
        # log record
        self.log_params()
        self.log_metrics()
        self.log_artifacts()

    def set_tag(self, value):
        self.client.set_tag(self.run.info.run_id, "checked", value)


