import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import json
from hashids import Hashids
from tqdm import tqdm

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
    i2i_id = 20000
    discord_mark_id = 0

try:
    # df = pd.read_parquet('output.parquet')
    df = pd.read_csv('output.csv', index_col=[0])
except:
    df = pd.DataFrame(columns=['img_path', 'thumbsup', 'thumbsdown', 'variation', 'url'])    
    df.set_index('img_path')

df['thumbsup'] = df['thumbsup'].astype(int)
df['thumbsdown'] = df['thumbsdown'].astype(int)
df['variation'] = df['variation'].astype(int)
df.info()


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
    # "{}{}-result/{}/{}_{:05d}.{}.jpg".format(image_url, task_type, dir_name, task_id,
    #                                          save_index, get_raw_from_id(task.id))
    for t in table:
        real_task_id = pd.read_sql_query(f'select raw_image from {t} where task_id = "{task_id}" and save_index = {save_index};', engine)
        if len(real_task_id):
            return real_task_id.loc[0, 'raw_image']
    return 'failed'

def read_variation():
    global i2i_id
    # Lost connection to MySQL server during query'
    print('reading from task_i2I')
    interval = 100
    while True:
        print(f'processing {i2i_id}~{i2i_id + interval}')
        task_i2I = pd.read_sql_query(f'select id, img_path from task_i2I where id > {i2i_id} and id <= {i2i_id+interval};', engine)
        if len(task_i2I) == 0:
            break
        for i in tqdm(range(len(task_i2I))):
            # raw = get_raw_from_id(task_i2I.loc[i, 'id'])
            link = task_i2I.loc[i, 'img_path']
            link = link.split('?')[0].split('/')[-1].split('%2F')[-1].split('.')[0]
            try:
                # 必须由 xxxxxx_0001组成
                task_id, save_index = link.split('_')
                assert len(task_id.split('-')) == 5
                int(save_index)
            except:
                continue
            if link not in df.index:
                df.loc[link] = {'thumbsup':0, 'thumbsdown':0, 'variation':0, 'url':''}
            df.loc[link, 'variation'] += 1
            if not df.loc[link, 'url']:
                # print(link)
                task_id, save_index = link.split('_')
                df.loc[link, 'url'] = get_url_from_task(task_id, int(save_index))


        print('len images:', len(df))
        i2i_id = int(task_i2I.loc[len(task_i2I)-1, 'id'])
        save_to_local()
    print(f'ending at id={i2i_id}')

def read_discord_mark():
    global discord_mark_id
    print('reading from discord_mark_item')
    interval = 100
    while True:
        print(f'processing {discord_mark_id}~{discord_mark_id + interval}')
        discord_mark_item = pd.read_sql_query(f'select * from discord_mark_item where id > {discord_mark_id} and id <= {i2i_id+interval};', engine)
        if len(discord_mark_item) == 0:
            break
        for i in tqdm(range(len(discord_mark_item))):
            task_id = discord_mark_item.loc[i, 'task_id']
            save_index = discord_mark_item.loc[i, 'save_index']
            mark = discord_mark_item.loc[i, 'mark']
            link = '{}_{:0>5d}'.format(task_id, save_index)
            if link not in df.index:
                df.loc[link] = {'thumbsup':0, 'thumbsdown':0, 'variation':0, 'url':''}
            if mark == -1:
                df.loc[link, 'thumbsdown'] += 1
            else:
                df.loc[link, 'thumbsup'] += 1

            if not df.loc[link, 'url']:
                df.loc[link, 'url'] = get_url_from_task(task_id, save_index)

        print('len images:', len(df))
        discord_mark_id = discord_mark_item.loc[len(discord_mark_item)-1, 'id']
        save_to_local()
    print(f'ending at id={discord_mark_id}')


def main():
    read_variation()
    read_discord_mark()

if __name__ == "__main__":
    # print(get_url_from_task('3feb1214-6497-11ed-af05-00163e025c94'))
    # print('url:', get_url_from_task('bca91484-a6a2-11ed-a561-00163e008cc0', 1))
    main()
    