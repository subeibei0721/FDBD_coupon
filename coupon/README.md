# O2O优惠券使用行为分析

## 项目描述
基于Hadoop MapReduce的O2O优惠券使用行为分析系统，对用户线上线下消费数据进行多维度统计分析。

## 实验任务
- **任务一**: 商家消费行为统计（正样本、负样本、普通消费）
- **任务二**: 商家周边活跃顾客数量统计（基于距离分析）
- **任务三**: 优惠券使用时间统计（热门优惠券平均使用间隔）
- **任务四**: 优惠券使用影响因素分析(基于折扣类型和力度进行统计分析)

## 技术栈
- Hadoop 3.3.4
- MapReduce
- Java 1.8
- Maven 3.9.11

## 项目结构
coupon/
├── output/
│   ├── task2_result
│   ├── task3_final_result.txt
│   ├── task3_temp_result.txt
│   ├── task4_result1.txt
│   └── task4_result2.txt
├── src/main/java/coupon/
│   ├── Task1_MerchantAnalysis.java # 任务一：商家分析
│   ├── Task2_MerchantDistanceAnalysis.java # 任务二：商家距离分析
│   ├── Task3_CouponTimeAnalysis.java # 任务三：优惠券时间分析（第一阶段）
│   ├── Task3_CouponFilterAndSort.java # 任务三：优惠券筛选排序（第二阶段）
│   ├── Task4_DiscountAnalysis.java # 任务四：折扣分析
│   └── Task4_DiscountRateAnalysis.java # 任务四：折扣率分析
├── target/ # 编译输出目录
├── pom.xml # Maven配置
└── README.md # 项目说明