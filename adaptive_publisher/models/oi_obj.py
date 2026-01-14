import torch

from adaptive_publisher.models.base import BaseModel

from adaptive_publisher.conf import (
    OBJ_MODEL_NAME,
)

class OIObjModel(BaseModel):
    def __init__(self, oi_label_list=None):
        super().__init__()
        if oi_label_list is None:
            oi_label_list = []
        self.oi_label_list = oi_label_list
        self.equiv_ois = {
            7: 2,
            5: 2,
        }
        self.total_oi_ids = len(oi_label_list)
        self.setup()

    def setup_oi_ids(self):
        self.oi_ids = set()
        names = {i: name for i, name in enumerate(self.model.names)}
        for class_idx, label in names.items():
            if label.lower() in self.oi_label_list:
                self.oi_ids.add(class_idx)

    def update_oi_ids(self, oi_label_list):
        self.oi_label_list = oi_label_list
        self.total_oi_ids = len(oi_label_list)
        self.setup_oi_ids()

    def setup(self):
        cpu_device = torch.device('cpu')
        self.model = torch.hub.load('/torchhome/hub/ultralytics_yolov5_master', OBJ_MODEL_NAME, pretrained=True, source='local')
        self.model.eval()
        self.setup_oi_ids()

    def predict(self, new_image_frame, last_key_frame=None, threshold=0.0):
        output = self.model([new_image_frame])
        output_predictions = output.xyxyn[0]
        np_predicts = output_predictions.cpu().numpy().astype("float32")

        return self.is_positive(np_predicts, threshold)

    def is_positive(self, np_predicts, threshold):
        found_oi_set = set()
        for row in np_predicts:
            # xmin, ymin, xmax, ymax, conf, class_idx = row
            conf = float(row[4])
            class_idx = int(row[5])
            # label = self.class_labels[class_idx]
            conf = float(conf)
            if conf > threshold:
                clean_class_idx = self.equiv_ois.get(class_idx, class_idx)
                if class_idx in self.oi_ids:
                    found_oi_set.add(clean_class_idx)
                    if len(found_oi_set) == self.total_oi_ids:
                        return True

        return False



