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

