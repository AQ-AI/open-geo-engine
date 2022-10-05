import logging
import string
from typing import List
from sklearn.utils.multiclass import type_of_target
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from open_geo_engine.config.model_settings import TrainModelConfig


logging.basicConfig(level=logging.INFO)


class ModelTrainer:
    def __init__(
        self,
        model_names_list: List,
        features_cols: List,
    ) -> None:
        self.model_names_list = model_names_list
        self.features-Cols

    @classmethod
    def from_dataclass_config(cls, config: TrainModelConfig) -> "ModelTrainer":
        return cls(
            model_names_list=config.model_names_list,
        )

    def train_all_models(self, cohort_id, features_labels_df, mlb_categories):
        logging.info("Training all models")
        for model_name in self.model_names_list:
            (
                train_model,
                model_id,
                validation_df,
                X_valid,
                Y_valid,
            ) = self.execute_one_model(
                cohort_id, model_name, features_labels_df, mlb_categories
            )
            return train_model, model_name, model_id, validation_df, X_valid, Y_valid

    def execute_one_model(
        self,
        train_df,
        model_name,
    ):
        """This is a docstring that describes the overall function:
        Arguments
        ---------
            model_id : str
                      A model_id that identifies the model based on the `cohort_id`,
                      `model_name` and `hyperparameters`
            train_model: model
                     model instance to use in training
            model_name: str
                      A name identifying the algorithm used

        Returns
        -------
            train_model: model
                     model instance to use in training
            mlb: MultiLabelBinarizer()
                      A label binariser to generate category matrix.
            model_id: str
                      A model_id that identifies the model based on the `cohort_id` and
                      `hyperparameters`
        """
        logging.info(f"Training model {model_name}")
        # split by training and validation
        X_train = train_df[self.features_cols].values

        # split by labels and features
        pipeline = self.get_train_pipeline(model_name)

        logging.info("Fitting model")
        train_model = self.fit_model(pipeline, X_train, Y_train)
        cv = self._cv_strategy()


        ix = [i for i in range(data.shape[1]) if i != 11]
        X = data[:, ix]

        scores = cross_validate(pipeline, X, y, scoring="recall_macro", cv=cv, n_jobs=-1)

        # get model_id

        return train_model, X_valid, Y_valid

    def get_train_pipeline(self, model_name):
        """
        Create pipeline based on model name and instantiation

        Arguments
        ---------
            model_name : dict

        Returns
        -------
            pipeline
            model_name : str
        """
        # argument from config to change the
        # model name and instantiation

        # include random state(?) in dictionary

        pipeline = Pipeline(
            [steps=
                ("imputer", self._impute_missing_value())
                (f"{model_name}", self._get_model(model_name)),
            ]
        )
        return pipeline

    def fit_model(self, text_clf, X_train, y_train):
        return text_clf.fit(X_train, y_train)

    def _cv_strategy(self,):
        return RepeatedStratifiedKFold(n_splits=10, n_repeats=3, random_state=1)


    def create_target_variable(self, train_df):
        le = preprocessing.LabelEncoder()
        train_df["categorical_label"] = le.transform(train_df[self.target_variable])
        return train_df["categorical_label"].values

    def _get_model(self, model_name):
        if model_name == "RFC":
            return RandomForestClassifier()
        else:
            logging.info(f"Model name {model_name} not exist")

    def _impute_missing_value(self, ):
        return SimpleImputer(add_indicator=True)


train_df = pd.read_csv("/home/ubuntu/unicef_work/open-geo-engine/local_data/joined_data/train_nulls.csv")
