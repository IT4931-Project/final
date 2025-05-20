#!/usr/bin/env python3
"""

model
- self_attention
- build_model
- model_creator để tạo mô hình cho BigDL Orca Estimator

"""

import os
import logging
import tensorflow as tf
from tensorflow.keras import layers, Model
from dotenv import load_dotenv

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/trainer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("model")

# Tải biến môi trường
load_dotenv()

# Các tham số mô hình từ biến môi trường
SEQUENCE_LENGTH = int(os.getenv('SEQUENCE_LENGTH', '30'))  # Số ngày nhìn lại trong quá khứ
FUTURE_DAYS = int(os.getenv('FUTURE_DAYS', '7'))  # Số ngày dự đoán
HIDDEN_UNITS = int(os.getenv('HIDDEN_UNITS', '64'))  # Mặc định là 64 như trong train.py
DROPOUT_RATE = float(os.getenv('DROPOUT_RATE', '0.1'))  # Mặc định là 0.1 như trong train.py
ATTENTION_HEADS = int(os.getenv('ATTENTION_HEADS', '4'))  # Mặc định là 4 như trong train.py

class SelfAttention(tf.keras.layers.Layer):
    """
    Triển khai lớp Self-Attention
    
    Lớp này triển khai cơ chế self-attention đa đầu (multi-head),
    cho phép mô hình tập trung vào các phần khác nhau của chuỗi đầu vào.
    """
    def __init__(self, embed_size, heads, dropout=0.1, forward_expansion=4):
        """
        Khởi tạo lớp self-attention
        
        Tham số:
            embed_size: Kích thước vector embedding
            heads: Số lượng đầu attention
            dropout: Tỷ lệ dropout để điều chuẩn
            forward_expansion: Hệ số mở rộng cho mạng feed-forward
        """
        super(SelfAttention, self).__init__()
        self.embed_size = embed_size
        self.heads = heads
        self.head_dim = embed_size // heads
        self.scale = tf.math.sqrt(tf.cast(self.head_dim, tf.float32))

        # Các lớp chiếu
        self.project_in = layers.Dense(embed_size * 3)
        self.project_out = layers.Dense(embed_size)

        # Chuẩn hóa lớp
        self.norm1 = layers.LayerNormalization(epsilon=1e-6)
        self.norm2 = layers.LayerNormalization(epsilon=1e-6)

        # Dropout để điều chuẩn
        self.dropout = layers.Dropout(dropout)

        # Mạng feed-forward
        self.feed_forward = tf.keras.Sequential([
            layers.Dense(forward_expansion * embed_size),
            layers.Dropout(dropout),
            layers.ReLU(),
            layers.Dense(embed_size),
            layers.Dropout(dropout)
        ])

    def call(self, x, training=False):
        """
        Lượt truyền tiến của lớp self-attention
        
        Tham số:
            x: Tensor đầu vào
            training: Có đang ở chế độ huấn luyện không (ảnh hưởng đến dropout)
            
        Trả về:
            Tensor đầu ra sau khi qua self-attention và feed-forward
        """
        batch_size = tf.shape(x)[0]
        seq_len = tf.shape(x)[1]

        # Chiếu tuyến tính để lấy query, key và value
        qkv = self.project_in(x)
        Q, K, V = tf.split(qkv, num_or_size_splits=3, axis=-1)

        # Reshape thành (batch, heads, seq_len, head_dim)
        Q = tf.reshape(Q, (batch_size, seq_len, self.heads, self.head_dim))
        K = tf.reshape(K, (batch_size, seq_len, self.heads, self.head_dim))
        V = tf.reshape(V, (batch_size, seq_len, self.heads, self.head_dim))

        # Chuyển vị ma trận để tính toán attention
        Q = tf.transpose(Q, perm=[0, 2, 1, 3])
        K = tf.transpose(K, perm=[0, 2, 1, 3])
        V = tf.transpose(V, perm=[0, 2, 1, 3])

        # Scaled Dot-Product Attention
        attn_weights = tf.matmul(Q, K, transpose_b=True) / self.scale
        attn_weights = tf.nn.softmax(attn_weights, axis=-1)
        attn_output = tf.matmul(attn_weights, V)

        # Nối các đầu và reshape
        attn_output = tf.transpose(attn_output, perm=[0, 2, 1, 3])
        attn_output = tf.reshape(attn_output, (batch_size, seq_len, self.embed_size))

        # Chiếu đầu ra với kết nối phần dư và chuẩn hóa
        out = self.project_out(attn_output)
        out = self.dropout(out, training=training)
        x = self.norm1(out + x)

        # Mạng feed-forward với kết nối phần dư và chuẩn hóa
        forward = self.feed_forward(x, training=training)
        out = self.norm2(forward + x)
        
        return out

def build_model(input_timesteps=SEQUENCE_LENGTH, output_timesteps=FUTURE_DAYS, features=1, lstm_units=HIDDEN_UNITS, heads=ATTENTION_HEADS):
    """
    Xây dựng mô hình LSTM hai chiều với SelfAttention
    
    Kiến trúc mô hình bao gồm:
    1. Bốn lớp LSTM hai chiều (bộ mã hóa - encoder)
    2. Lớp Self-attention
    3. Bốn lớp LSTM hai chiều (bộ giải mã - decoder)
    4. Lớp Dense cho đầu ra
    
    Tham số:
        input_timesteps: Số bước thời gian nhìn lại quá khứ
        output_timesteps: Số bước thời gian cần dự đoán
        features: Số lượng đặc trưng đầu vào
        lstm_units: Số lượng đơn vị LSTM
        heads: Số lượng đầu attention
        
    Trả về:
        Một instance của mô hình Keras
    """
    logger.info(f"Building model with {input_timesteps} input timesteps, {output_timesteps} output timesteps, {features} features, {lstm_units} LSTM units, and {heads} attention heads")
    
    # Lớp đầu vào
    inputs = layers.Input(shape=(input_timesteps, features))

    # Bộ mã hóa (4 lớp LSTM hai chiều)
    x = inputs
    for _ in range(4):
        x = layers.Bidirectional(layers.LSTM(lstm_units, return_sequences=True))(x)

    # Lớp Self-Attention
    x = SelfAttention(embed_size=2 * lstm_units, heads=heads)(x)

    # Lấy đầu ra của bước thời gian cuối cùng và lặp lại cho đầu vào của bộ giải mã
    x = layers.RepeatVector(output_timesteps)(x[:, -1, :])

    # Bộ giải mã (4 lớp LSTM hai chiều)
    for _ in range(4):
        x = layers.Bidirectional(layers.LSTM(lstm_units, return_sequences=True))(x)

    # Lớp đầu ra
    output = layers.TimeDistributed(layers.Dense(1))(x)

    # Tạo mô hình
    model = Model(inputs, output)
    logger.info("Model built successfully")
    
    return model

def model_creator(config):
    """
    Hàm tạo mô hình cho BigDL Orca Estimator
    
    Tham số:
        config: Từ điển cấu hình
        
    Trả về:
        Instance của mô hình Keras
    """
    features = config.get("features", 1)
    lstm_units = config.get("lstm_units", HIDDEN_UNITS)
    heads = config.get("heads", ATTENTION_HEADS)
    
    return build_model(
        input_timesteps=SEQUENCE_LENGTH,
        output_timesteps=FUTURE_DAYS,
        features=features,
        lstm_units=lstm_units,
        heads=heads
    )
