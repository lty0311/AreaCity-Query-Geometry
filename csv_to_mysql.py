import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import time
import logging


# 必须是mysql5.7 my.ini增加max_allowed_packet=64M
# mysql查询方式：SELECT id,deep,name,ext_path FROM geo WHERE ST_Intersects(polygon, ST_GeomFromText('POINT(123.42805 41.834777)',4326))=1

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('geo_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': '3307',
    'user': 'root',
    'password': 'root',
    'database': 'area_city_geo',
    'charset': 'utf8mb4'
}

# 目标表和文件
TABLE_NAME = 'geo'
CSV_FILE = 'ok_geo.csv'
CHUNKSIZE = 50000  # 根据内存调整

def validate_coordinate(coord_str):
    """验证坐标字符串是否为有效的 'x y' 格式"""
    try:
        parts = coord_str.strip().split()
        if len(parts) != 2:
            return False
        float(parts[0]), float(parts[1])  # 尝试转换为浮点数
        return True
    except:
        return False

def prepare_polygon(polygon_str):
    """将原始polygon字符串转换为有效的WKT格式"""
    if not polygon_str or not isinstance(polygon_str, str):
        return None
    
    try:
        # 清理数据：移除所有括号和多余空格
        clean_str = polygon_str.strip() \
            .replace("(", "").replace(")", "") \
            .replace("[", "").replace("]", "")
        
        # 分割坐标点并验证
        coords = []
        for coord in clean_str.split(","):
            coord = coord.strip()
            if validate_coordinate(coord):
                coords.append(coord)
        
        # 需要至少3个有效坐标点才能构成多边形
        if len(coords) < 3:
            logger.warning(f"坐标点不足3个: {polygon_str}")
            return None
        
        # 确保多边形闭合（首尾坐标相同）
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        
        # 再次检查坐标数量（闭合后至少需要4个点）
        if len(coords) < 4:
            logger.warning(f"闭合后坐标点不足4个: {polygon_str}")
            return None
        
        return f"POLYGON(({','.join(coords)}))"
    except Exception as e:
        logger.error(f"处理polygon失败: {polygon_str} | 错误: {str(e)}")
        return None

def prepare_point(point_str):
    """将原始geo字符串转换为有效的POINT WKT格式"""
    if not point_str or not isinstance(point_str, str):
        return None
    
    try:
        if validate_coordinate(point_str):
            return f"POINT({point_str})"
        return None
    except:
        return None

def import_data():
    """主导入函数"""
    # 创建数据库引擎
    engine = create_engine(
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
        pool_pre_ping=True,
        echo=False  # 设为True可查看所有SQL语句（调试用）
    )
    
    # 记录开始时间
    start_time = time.time()
    total_rows = 0
    success_rows = 0
    failed_rows = 0
    
    try:
        # 分块读取CSV文件
        for chunk_idx, chunk in enumerate(pd.read_csv(CSV_FILE, chunksize=CHUNKSIZE)):
            chunk_start = time.time()
            values = []
            bad_records = []
            
            # 预处理数据
            for _, row in chunk.iterrows():
                try:
                    point_wkt = prepare_point(str(row['geo'])) if pd.notna(row['geo']) else None
                    polygon_wkt = prepare_polygon(str(row['polygon'])) if pd.notna(row['polygon']) else None
                    
                    record = {
                        'id': int(row['id']),
                        'pid': int(row['pid']),
                        'deep': int(row['deep']),
                        'name': str(row['name']),
                        'ext_path': str(row['ext_path']),
                        'geo': point_wkt,
                        'polygon': polygon_wkt
                    }
                    
                    # 记录无效数据
                    if pd.notna(row['polygon']) and not polygon_wkt:
                        bad_records.append({
                            'id': row['id'],
                            'reason': 'Invalid polygon format',
                            'data': row['polygon']
                        })
                    
                    values.append(record)
                except Exception as e:
                    logger.error(f"预处理行失败: {row.to_dict()} | 错误: {str(e)}")
                    failed_rows += 1
            
            # 批量插入
            if values:
                try:
                    with engine.begin() as conn:
                        # 使用executemany批量插入
                        conn.execute(
                            text(f"""
                            INSERT INTO {TABLE_NAME} 
                            (id, pid, deep, name, ext_path, geo, polygon)
                            VALUES (
                                :id, :pid, :deep, :name, :ext_path,
                                ST_GeomFromText(:geo, 4326),
                                ST_GeomFromText(:polygon, 4326)
                            )
                            """),
                            values
                        )
                        success_rows += len(values)
                except Exception as e:
                    logger.error(f"批量插入失败: {str(e)}")
                    failed_rows += len(values)
            
            # 记录无效数据到文件
            if bad_records:
                with open('bad_polygons.csv', 'a') as f:
                    for rec in bad_records:
                        f.write(f"{rec['id']},{rec['reason']},\"{rec['data']}\"\n")
            
            # 进度报告
            chunk_time = time.time() - chunk_start
            total_rows += len(chunk)
            logger.info(
                f"批次 {chunk_idx + 1} | "
                f"成功: {len(values)} | 失败: {len(chunk) - len(values)} | "
                f"耗时: {chunk_time:.2f}s | "
                f"本批速度: {len(chunk)/chunk_time:.2f} rows/s"
            )
    
    except Exception as e:
        logger.critical(f"导入过程异常终止: {str(e)}", exc_info=True)
    finally:
        engine.dispose()
    
    # 最终报告
    total_time = time.time() - start_time
    logger.info(
        f"\n{'='*50}\n"
        f"导入完成!\n"
        f"总行数: {total_rows}\n"
        f"成功插入: {success_rows}\n"
        f"失败行数: {failed_rows}\n"
        f"总耗时: {total_time:.2f}秒\n"
        f"平均速度: {total_rows/total_time:.2f} rows/s\n"
        f"{'='*50}"
    )

if __name__ == '__main__':
    logger.info("开始导入地理数据...")
    import_data()
    
