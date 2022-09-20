from sklearn.utils.multiclass import type_of_target
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.impute import SimpleImputer


model = RandomForestClassifier()
imputer = SimpleImputer(add_indicator=True)
pipeline = Pipeline(steps=[('i', imputer), ('m', model)])
cv = RepeatedStratifiedKFold(n_splits=10, n_repeats=3, random_state=1)

train_df = pd.read_csv("/home/ubuntu/unicef_work/open-geo-engine/local_data/joined_data/train_nulls.csv")
data = train_df[
    [
        "Temperature",
        "Humidity",
        "feels_like",
        "temp_min",
        "temp_max",
        "pressure",
        "wind_speed",
        "wind_deg",
        "rain",
        "clouds_all",
        "weather",
        "AQI",
        "longitude",
        "latitude",
        "NO2_column_number_density",
        "cloud_fraction",
        "time",
        "AOD_Uncertainty",
        "Column_WV",
        "Optical_Depth_047",
        "Optical_Depth_055",
        "B2",
        "B3",
        "B4",
        "avg_rad",
    ]
].values
le = preprocessing.LabelEncoder()
train_df["categorical_label"] = le.transform(train_df["AQI"])
y = train_df["categorical_label"].values


ix = [i for i in range(data.shape[1]) if i != 11]
X = data[:, ix]

scores = cross_validate(pipeline, X, y, scoring="recall_macro", cv=cv, n_jobs=-1)
