# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt

# 横坐标
x = [1, 4, 8, 16, 24, 36]

# 对应的值
y1 = [1.375401, 5.319482, 10.277482, 18.842023, 25.402852, 26.217749]
y2 = [49.514436, 47.875338, 46.248669, 42.39455175, 38.104278, 26.217749]

# 创建一个新的图形
plt.figure()

# 添加两条折线
plt.plot(x, y1, label='Line 1')
plt.plot(x, y2, label='Line 2')

# 添加图例
plt.legend()

# 显示图形
plt.show()