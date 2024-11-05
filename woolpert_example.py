import ee
from google.auth import default
import time
import random


def initialize_gee():
    credentials, project = default()
    print(f"Initializing GEE with project {project}")
    ee.Initialize(credentials, project=project)

class TaskStatus:
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCEL_REQUESTED = 'CANCEL_REQUESTED'
    CANCELLED = 'CANCELLED'

class TasksManager:
    def __init__(self, tasks, logger=None):
        self.tasks = tasks
        self.logger = logger
        self.paths = []

    def poll(self, max_retries=5, backoff_factor=2):
        retries = 0
        while any(task.active() for task in self.tasks):
            try:
                active_tasks = self.tasks
                running_exports = sum(1 for t in active_tasks if t.task_type ==
                                      ee.batch.Task.Type.EXPORT_IMAGE and t.status()['state'] == 'RUNNING')
                print(f"Currently running export tasks: {running_exports} / {len(active_tasks)}")

                for task in self.tasks:
                    if task.status() == TaskStatus.FAILED:
                        self.logger.error(f"Task {task.id} failed")
                        return False
                time.sleep(10)
                retries = 0  # Reset retries after a successful poll
            except (ConnectionError, TimeoutError) as e:
                self.logger.warning(f"Polling interrupted: {str(e)}")
                if retries < max_retries:
                    retries += 1
                    sleep_time = backoff_factor ** retries + random.uniform(0, 1)
                    self.logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                else:
                    self.logger.error("Max retries reached. Aborting polling.")
                    return False
        return all(task.status() == TaskStatus.COMPLETED for task in self.tasks)

    def get_export_destination(self, tasks=None):
        if tasks:
            self.tasks = tasks
        for task in self.tasks:
            if "assetExportOptions" in task.config:
                self.paths.append(
                    task.config["assetExportOptions"]["earthEngineDestination"]["name"]
                )
            elif "cloudStorageDestination" in task.config["fileExportOptions"]:
                file_format = task.config["fileExportOptions"]["fileFormat"].lower().replace("_", "")
                if file_format == "geotiff":
                    file_format = "tif"
                elif file_format == "tfrecordimage":
                    file_format = "tfrecord"
                self.paths.append(
                    f"{task.config['fileExportOptions']['cloudStorageDestination']['filenamePrefix']}"
                    f".{file_format}"
                )
            else:
                e = ValueError(f"Unsupported export destination {task.config}")
                print(e)
                if self.logger:
                    self.logger.error(e)
                raise e


def export_im_collection(
        im_collection, description, export_options, property="system:index", logger=None
):
    tasks = []
    for idx in im_collection.aggregate_array(property).getInfo():
        im = im_collection.filterMetadata(property, "equals", idx).first()
        _export_options = export_options.copy()
        if "fileNamePrefix" not in _export_options:
            _export_options["fileNamePrefix"] = idx
        else:
            _export_options["fileNamePrefix"] = (
                f"{_export_options['fileNamePrefix']}/{idx}"
            )
        task = ee.batch.Export.image.toCloudStorage(
            image=im, description=description, **_export_options
        )
        task.start()
        print(f"Exporting image {idx}...")
        tasks.append(task)
    return TasksManager(tasks, logger)


if __name__ == "__main__":
    initialize_gee()
    bucket_name = "crop-identification"
    geometry = ee.Geometry.Point([12.4924, 41.8902])
    start_date = "2022-01-01"
    end_date = "2022-02-01"
    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(geometry)
        .filterDate(start_date, end_date)
        .select(["B4", "B3", "B2"])
        .map(lambda image: image.multiply(0.0001))
    )

    region = geometry.buffer(5000).bounds(10)

    tasks_manager = export_im_collection(
        collection,
        "test_s2",
        {
            "scale": 10,
            "region": region,
            "maxPixels": 10e10,
            "fileFormat": "GeoTIFF",
            "fileNamePrefix": "woolpert_example",
            "bucket": bucket_name,
        },
    )
    tasks_manager.poll()
    tasks_manager.get_export_destination()
    print(tasks_manager.paths)
