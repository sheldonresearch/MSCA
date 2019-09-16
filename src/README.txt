results_part1: 改变spammer的比例主算法
results_CA_part1: 改变spammer比例的对比算法

results_part2: 改变spam的比例主算法
results_CA_part2: 改变spam比例的对比算法

results_part3: 改变train_per的比例主算法
results_CA_part3: 改变train_per比例的对比算法



已经做过的实验记录：
train_per= 0.8 spammer_per= 0.03 reset_dataset= True dump= True add_unknown_into_model= False
train_per= 0.8 spammer_per= 0.07 reset_dataset= True dump= True add_unknown_into_model= False
train_per= 0.8 spammer_per= 0.11 reset_dataset= True dump= True add_unknown_into_model= False
train_per= 0.8 spammer_per= 0.15 reset_dataset= True dump= True add_unknown_into_model= False
train_per= 0.8 spammer_per= 0.19 reset_dataset= True dump= True add_unknown_into_model= False
train_per= 0.8 spammer_per= 0.23 reset_dataset= True dump= True add_unknown_into_model= False







请注意：因为代码的原因， 你固定一个spammer_per后， 要把所有算法都跑完然后才能改变spamme_per
否则每一次运行训练集和测试集是不一样的。为此第一阶段你可以对每一算法做变化spammer_per的改写，然后在
main函数中统一调用所有算法，同步将结果投射到对应的目录中去。

第1步：
1.1 运行src/algorithm/user_classify.py
1.1.1 user_class.run(train_per=0.5,reset_dataset=True,dump=True)
得到用户和微博的先验类别，将结果保存到src/main/prior/user_prior.txt和src/main/prior/user_train.txt
打印输出准确率，召回率和F1值
1.1.2 user_class.evalutaion()
将更多的评价指标测试结果保存到src/main/lr/， 包括top, ap, roc,tt


数据库中
final_user: 3843个用户， 水军903， 非水军2940
normal： 13906个用户， 水军和非水军未知, 水军和非水军未知，为此我们通过人工的方法从从这些用户中挑选了一些正常的用户，标记为choose='yes'
但是choose='not'的并不意味着是水军，他们只是还没有被人工标记出来。
spammer: 892个水军用户


1.2 运行和src/algorithm/wblog_classify.py






这是本人大论文的实验代码
取名CMSCA，全程 crowdsourcing microblogs and spammers co-detection algorithm
（代码中取名为msca是历史遗留原因，一开始想叫msca的，后来才改为cmsca）

###########################我是分割线##################################


algorithm包中是各算法的实现

dataset包中的dataset.py是用来抽取实验数据集的

user包读取用户数据，计算用户先验类别

microblog包读取微博数据，计算微博先验类别

util包中是一些工具类，以及统计为了画累积分布图的数据


###########################我是分割线##################################


main包中：
crowd_target，detect_vc，lr，s3mcd这几个文件夹下是各算法的数据结果的txt文件
prior是先验类别计算的结果，user_prior.txt中是测试数据，每列含义为“用户id， 真实类别，先验类别，分类概率输出值”，user_train.txt中是用户训练集，每列含义为“用户id， 真实类别”；user_prior.txt和user_train.txt同理
relation_intensity是预先生成的一些矩阵，供msca算法使用，因为不可能每跑一次算法都要读一次数据库
main.py是所有算法的入口，通过对里面代码的简单修改（注释和去除注释）可以跑不同算法
evaluation.py是一个工具类，用于评价算法效果


###########################我是分割线##################################

代码中有一些读取数据库的操作，注意为了能顺利运行需要保证有相应的数据库表

代码中的注释应该还是有不少的，应该有助于理解代码