import os
import warnings

import numpy as np

import torch
import torch.nn as nn
import torchvision.models as models

from adaptive_publisher.models.base import BaseModel
from adaptive_publisher.conf import (
    MODELS_PATH,
    CLS_MODEL_ID,
)


class OIClsModel(BaseModel):
    def __init__(self, oi_cls_index=1):
        super().__init__()
        self.oi_cls_index = oi_cls_index
        self.device = torch.device("cpu")
        self.model = None
        self.model_loaded = False
        self.setup()

    def setup(self):
        num_classes = 2
        base_model = self.get_base_fine_tuned_model(
            models.mobilenet_v3_large(), num_classes, freeze=False
        )

        model_path = os.path.join(MODELS_PATH, f"{CLS_MODEL_ID}.pth")

        try:
            if not os.path.exists(model_path):
                raise FileNotFoundError(model_path)

            # Load the saved state dictionary
            state = torch.load(model_path, map_location=self.device)
            base_model.load_state_dict(state)

            self.model = base_model.to(self.device)
            self.model.eval()
            self.model_loaded = True

        except Exception as e:
            # Fail-soft: start without the classifier model
            self.model = None
            self.model_loaded = False
            warnings.warn(
                f"[AdaptivePublisher] OI classifier model not loaded: {e}. "
                f"Running in NO-MODEL mode: predict() returns neutral score 0.5."
            )

    def get_base_fine_tuned_model(self, base_model, num_classes=2, freeze=True):
        base_model.classifier[-1] = nn.Linear(
            base_model.classifier[-1].in_features, num_classes
        )
        return base_model

    def predict(self, new_image_frame, last_key_frame=None):
        # If model isn't available, return a neutral probability so the pipeline can keep running
        if not self.model_loaded or self.model is None:
            return 0.5

        input_batch = new_image_frame.unsqueeze(0)
        prediction = self.model(input_batch).squeeze(0).softmax(0)
        class_probs = prediction.tolist()
        return class_probs[self.oi_cls_index]
