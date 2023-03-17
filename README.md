# extract-image

需求：直接给一个set图片链接以及里面每张图赞和踩的数量
1. 针对图片粒度（不关心同一任务中不同图片的比较关系）
2. 只采集原始数据，不做后处理
输出格式：一行一张图片标签（taskid_index），列分别为点赞次数、点踩次数、i2i次数、图片链接

步骤：
1. 访问数据表：task_i2I，提取i2i数据
2. 访问数据表：discord_mark_item，discord用户评价数据
3. 提取过程中查询oss_item表，将(task_id, save_index)转为原始图像地址，为'failed'或''则说明未查询到该图片

第一次开始运行时，取i2i_id=20000，因为前20000个任务均在2022年，不用取出

脚本运行方式：`python extract.py`，支持多次（断点）执行，每次会将两个数据库的id保存下来

## 3.17更新

为了避免过早的数据格式不正确：
- task_i2I，从`2023-01-18 15:59:13`（id=50001）开始，i2i_id初始值为50000
- discord_mark_item，从`2023-01-16 22:08:32`（id=1）开始

除了原来的条目以外，还需要获取：
- sensitive_flag, sensitive_rating, age_rating（oss_item中）
- prompt
  - task_i2I任务则直接获取prompt
  - discord_mark_item需要根据task_id查询其任务类型以及任务对应的prompt，因为discord_mark_item表并没有task_type字段，需要在task_i2I、task_t2I中分别尝试查询
- source
  - task_i2I任务指定为：task_i2I
  - discord_mark_item任务可能是i2i或者t2i：task_i2I, task_t2I


## 字段说明

- img_path：task_id与save_index用下划线连接在一起，是图片的唯一标识符
- thumbsup：点赞次数
- thumbsdown：点踩次数
- variation：该图进行t2i次数
- url：图片的地址
- sensitive_flag
- sensitive_rating：图片推理的时候得到的关于涉黄评分
- age_rating：图片推理的时候得到的关于涉幼评分
- source：图片任务（t2i/i2i）
- prompt：图片的提示词


多线程说明：
- interval = 50     单个线程处理数据条数
- workers = 30      线程池大小
- lag = 0.5         两个线程之间的时间差