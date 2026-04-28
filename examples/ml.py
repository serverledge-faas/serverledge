import os

# Suppress TF logs and oneDNN info
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import tensorflow as tf


def full_demo_with_metrics(epochs):
    # This is a small test that uses all the available libraries of the python312ml runtime.

    # Generate random data (NumPy)
    x = np.linspace(0, 10, 100)
    y = 4 * x + 7 + np.random.randn(100)

    # Generate random test data
    x_test = np.linspace(0, 10, 20)
    y_test = 4 * x_test + 7 + np.random.randn(20)

    # Pandas DataFrame
    df = pd.DataFrame({"x": x, "y": y})
    df_test = pd.DataFrame({"x": x_test, "y": y_test})

    # use SciPy
    corr, p_value = stats.pearsonr(df["x"], df["y"])

    # use Scikit-learn
    sklearn_model = LinearRegression()
    sklearn_model.fit(df[["x"]], df["y"])
    sklearn_preds = sklearn_model.predict(df_test[["x"]])

    sklearn_mse = mean_squared_error(df_test["y"], sklearn_preds)

    # Use tenseorflow (cpu only) with keras
    tf_model = tf.keras.Sequential([
        tf.keras.Input(shape=(1,)),
        tf.keras.layers.Dense(8, activation="relu"),
        tf.keras.layers.Dense(1)
    ])

    tf_model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
                     loss="mse")

    tf_model.fit(df[["x"]], df["y"], epochs=epochs, verbose=0)
    tf_preds = tf_model.predict(df_test[["x"]], verbose=0).flatten()

    tf_mse = mean_squared_error(df_test["y"], tf_preds)

    return {
        "correlation_train": corr,
        "p_value_train": p_value,
        "sklearn_sample_pred": sklearn_preds[:5].tolist(),
        "sklearn_mse": sklearn_mse,
        "tensorflow_sample_pred": tf_preds[:5].tolist(),
        "tensorflow_mse": tf_mse,
    }

def handler(params, context):
    epochs = 200
    if "n" in params:
        epochs = int(params["n"])
    print("Will run with", epochs, "epochs")

    result = full_demo_with_metrics(epochs)
    print("=== RESULTS ===")
    for key, value in result.items():
        print(f"{key}: {value}")
    return result
