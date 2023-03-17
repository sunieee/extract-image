import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import json
from hashids import Hashids
from tqdm import tqdm
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


class JsonEncoder(json.JSONEncoder):
    """Convert numpy classes to JSON serializable objects."""

    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating, np.bool_)):
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(JsonEncoder, self).default(obj)
        

# 初始化数据库连接
# 按实际情况依次填写MySQL的用户名、密码、IP地址、端口、数据库名
engine = create_engine('mysql+pymysql://root:3Mkkwc.Gi2HAbJ@8.209.252.109:3306/bakamaka_beta')

# 每次运行完成之后，需要更改i2i_id和discord_mark_id
try:
    with open('output.json', 'r') as f:
        output = json.load(f)
    i2i_id = output['i2i_id']
    discord_mark_id = output['discord_mark_id']
    print(f'continue process at i2i_id={i2i_id}, discord_mark_id={discord_mark_id}')
except:
    i2i_id = 50000
    discord_mark_id = 0

try:
    # df = pd.read_parquet('output.parquet')
    df = pd.read_csv('output.csv', index_col=[0])
except:
    df = pd.DataFrame(columns=['img_path', 'thumbsup', 'thumbsdown', 'variation', 'url', \
                               'sensitive_flag', 'sensitive_rating', 'age_rating', 'source', 'prompt'])    
    df.set_index('img_path')

df['thumbsup'] = df['thumbsup'].astype(int)
df['thumbsdown'] = df['thumbsdown'].astype(int)
df['variation'] = df['variation'].astype(int)
df.info()
interval = 50
workers = 30
lag = 0.5


def save_to_local():
    output = {
        'i2i_id': i2i_id,
        'discord_mark_id': discord_mark_id
    }
    with open('output.json', 'w') as f:
        json.dump(output, f, ensure_ascii=False, cls=JsonEncoder)
    # df.to_parquet('output.parquet')
    df.to_csv('output.csv')


salt = "bakamaka_pic_raw_salt"
hash_tool = Hashids(salt)

def get_raw_from_id(id_num):
    return hash_tool.encode(id_num + 10000000000)

def get_url_from_task(task_id, save_index, table=['oss_item']):
    # 根据task_id和save_index在oss_item中获取图片地址和rating分数
    # "{}{}-result/{}/{}_{:05d}.{}.jpg".format(image_url, task_type, dir_name, task_id,
    #                                          save_index, get_raw_from_id(task.id))
    for t in table:
        task = pd.read_sql_query(f'select raw_image, sensitive_flag, sensitive_rating, age_rating from {t} where task_id = "{task_id}" and save_index = {save_index};', engine)
        if len(task):
            return {
                'url': task.loc[0, 'raw_image'],
                'sensitive_flag': task.loc[0, 'sensitive_flag'], 
                'sensitive_rating': task.loc[0, 'sensitive_rating'], 
                'age_rating': task.loc[0, 'age_rating']
            }
    return {}

def tostring(s):
    return s.replace(',', ';').replace('\n', '|')

def get_task_from_task_id(task_id):
    # 根据task_id在 task_i2I或task_t2I 中获取prompt
    for t in ['task_i2I', 'task_t2I']:
        prompt = pd.read_sql_query(f'select prompt from {t} where task_id = "{task_id}";', engine)
        if len(prompt):
            return {
                'source': t, 
                'prompt': tostring(prompt.loc[0, 'prompt'])
            }
    return {}


def read_variation_once(i2i_id):
    doc = defaultdict(dict)
    print(f'processing {i2i_id}~{i2i_id + interval}')
    task_i2I = pd.read_sql_query(f'select id, img_path, prompt from task_i2I where id > {i2i_id} and id <= {i2i_id+interval};', engine)
    if len(task_i2I) == 0:
        return doc
    for i in tqdm(range(len(task_i2I))):
        # print(i)
        # raw = get_raw_from_id(task_i2I.loc[i, 'id'])
        link = task_i2I.loc[i, 'img_path']
        prompt = tostring(task_i2I.loc[i, 'prompt'])
        # print(prompt)
        link = link.split('?')[0].split('/')[-1].split('%2F')[-1].split('.')[0]
        try:
            # 必须由 xxxxxx_0001组成
            task_id, save_index = link.split('_')
            assert len(task_id.split('-')) == 5
            int(save_index)
        except:
            continue
        if link not in doc:
            doc[link] = {'thumbsup':0, 'thumbsdown':0, 'variation':0}
        doc[link]['variation'] += 1
        doc[link]['source'] = 'task_i2I'
        doc[link]['prompt'] = prompt

        if 'url' not in doc[link] and link not in df.index:
            # print(link)
            task_id, save_index = link.split('_')
            doc[link].update(get_url_from_task(task_id, int(save_index)))
    # print(doc)
    return doc


def read_discord_mark_once(discord_mark_id):
    doc = defaultdict(dict)
    print(f'processing {discord_mark_id}~{discord_mark_id + interval}')
    discord_mark_item = pd.read_sql_query(f'select * from discord_mark_item where id > {discord_mark_id} and id <= {discord_mark_id+interval};', engine)
    if len(discord_mark_item) == 0:
        return doc
    for i in tqdm(range(len(discord_mark_item))):
        task_id = discord_mark_item.loc[i, 'task_id']
        save_index = discord_mark_item.loc[i, 'save_index']
        mark = discord_mark_item.loc[i, 'mark']
        link = '{}_{:0>5d}'.format(task_id, save_index)
        if link not in doc:
            doc[link] = {'thumbsup':0, 'thumbsdown':0, 'variation':0}
        if mark == -1:
            doc[link]['thumbsdown'] += 1
        else:
            doc[link]['thumbsup'] += 1
        
        if 'prompt' not in doc[link] and link not in df.index:
            doc[link].update(get_url_from_task(task_id, save_index))
            doc[link].update(get_task_from_task_id(task_id))
    return doc


def read_table(func, id):
    executor = ThreadPoolExecutor(max_workers=workers)
    tasks = []

    while True:
        tasks.append(executor.submit(func, (id)))
        id += interval
        time.sleep(lag)
        if tasks[-1].done():    # 很快就退出，没有相应数据
            if len(tasks[-1].result()) == 0:
                break
        
    for future in as_completed(tasks):
        doc = future.result()
        for k, dic in doc.items():
            if k in df.index:
                for ix in ['thumbsup', 'thumbsdown', 'variation']:
                    df.loc[k, ix] += dic[ix]
                    dic.pop(ix)
                df.loc[k, list(dic.keys())] = list(dic.values())
            else:
                df.loc[k] = dic

        print('len images:', len(df))
    return id


def read_variation():
    global i2i_id
    # Lost connection to MySQL server during query'
    print('reading from task_i2I')
    i2i_id = read_table(read_variation_once, i2i_id)
    task_i2I = pd.read_sql_query(f'select id from task_i2I where id >= {i2i_id*0.8} and id <= {i2i_id};', engine)
    i2i_id = int(task_i2I.loc[len(task_i2I)-1, 'id'])
    print(f'ending at id={i2i_id}')
    save_to_local()


def read_discord_mark():
    global discord_mark_id
    print('reading from discord_mark_item')
    discord_mark_id = read_table(read_discord_mark_once, discord_mark_id)
    discord_mark_item = pd.read_sql_query(f'select id from discord_mark_item where id <= {discord_mark_id};', engine)
    discord_mark_id = int(discord_mark_item.loc[len(discord_mark_item)-1, 'id'])
    print(f'ending at id={discord_mark_id}')
    save_to_local()


def main():
    read_variation()
    read_discord_mark()

if __name__ == "__main__":
    # print(get_url_from_task('3feb1214-6497-11ed-af05-00163e025c94'))
    # print('url:', get_url_from_task('bca91484-a6a2-11ed-a561-00163e008cc0', 1))
    main()
    