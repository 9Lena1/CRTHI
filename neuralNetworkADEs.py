# TensorFlow and tf.keras
import tensorflow as tf


import os

from tensorflow.contrib.learn.python.learn.estimators._sklearn import train_test_split

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

# Metadata describing the text columns
COLUMNS = ['hdm_id', 'URGENT', 'NEWBORN', 'ELECTIVE', 'EMERGENCY', 'male', 'female', 'age',
           '0', '1', '2', '3', '3A', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16',
           'ADEs',
           'LG100-4', 'LG103-8', 'LG105-3', 'LG106-1', 'LG27-5', 'LG41751-5', 'LG41762-2', 'LG41808-3', 'LG41809-1',
           'LG41811-7', 'LG41812-5', 'LG41813-3', 'LG41814-1', 'LG41816-6', 'LG41817-4', 'LG41818-2', 'LG41820-8',
           'LG41821-6', 'LG41822-4', 'LG41855-4', 'LG47-3', 'LG50067-4', 'LG55-6', 'LG66-3', 'LG68-9', 'LG70-5',
           'LG74-7', 'LG78-8', 'LG80-4', 'LG85-3', 'LG88-7', 'LG89-5', 'LG90-3', 'LG92-9', 'LG96-0', '', 'LG97-8',
           'LG99-4',
           'A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15',
           'A16', 'B01', 'B02', 'B03', 'B05', 'B06', 'C01', 'C02', 'C03', 'C04', 'C05', 'C07', 'C08', 'C09', 'C10',
           'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D08', 'D09', 'D10', 'D11', 'G01', 'G02', 'G03', 'G04',
           'H01', 'H02', 'H03', 'H04', 'H05', 'J01', 'J02', 'J04', 'J05', 'J06', 'J07', 'L01', 'L02', 'L03', 'L04',
           'M01', 'M02', 'M03', 'M04', 'M05', 'M09', 'N01', 'N02', 'N03', 'N04', 'N05', 'N06', 'N07', 'P01', 'P02',
           'P03', 'R01', 'R02', 'R03', 'R05', 'R06', 'R07', 'S01', 'S02', 'S03', 'V01', 'V03', 'V04', 'V06', 'V07',
           'V08', 'V09', 'V10', 'V20']
FIELD_DEFAULTS = [[0]] * 158


def _parse_line(line):
    # Decode the line into its fields
    fields = tf.decode_csv(line, FIELD_DEFAULTS)
    # Pack the result into a dictionary
    features = dict(zip(COLUMNS, fields))
    # Separate the label from the features
    label = features.pop('ADEs')
    return features, label


def main():
    # Load Data
    train_path = "/Users/lenamondrejevski/Workspace/U/CRTHI/Project/Tensorflow/preProcessedNN.csv"
    dataset = tf.data.TextLineDataset(train_path)
    print(dataset)

    # Map dataset
    dataset = dataset.map(_parse_line)



    dataset = dataset.shuffle(buffer_size=256)
    repeat_count = 1
    dataset = dataset.repeat(repeat_count)
    dataset = dataset.batch(32)
    iterator = dataset.make_one_shot_iterator()
    features, labels = iterator.get_next()

    print(dataset)

    # Split dataset into train and test set
    x_train, x_test, y_train, y_test = train_test_split(features,
                                                        labels,
                                                        test_size=0.33)

    # Define Model:
    # Input Layer
    inputs = tf.keras.Input(shape=(154,))
    # Hidden Layers
    x1 = tf.keras.layers.Dense(127)(inputs)
    x2 = tf.keras.layers.Dense(64)(x1)
    x3 = tf.keras.layers.Dense(32)(x2)
    # Output Layers
    outputs = tf.keras.layers.Dense(1)(x3)
    model = tf.keras.Model(inputs=inputs, outputs=outputs)

    # Compile Model:
    model.compile(optimizer='adam',
                  loss='sparse_categorical_crossentropy',
                  metrics=['accuracy'])

    # Fit Model:
    model.fit(x_train, y_train, epochs=5)

    # Evaluate Model:
    model.evaluate(x_test, y_test)

if __name__ == '__main__':
    main();

