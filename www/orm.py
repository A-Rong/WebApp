# -*- coding: utf-8 -*-
# @Time    : 2018/3/9 21:10

import asyncio, logging
import aiomysql

def log(sql, args=()):
    logging.info('SQL: %s' % sql)       # 将我们所需要了解的信息打印到log中，方便调式

@asyncio.coroutine
def create_pool(loop, **kw):
    '''创建连接池'''
    logging.info('create datebase connection pool')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw .get('port', '3306'),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        charset = kw.get('charset','utf-8'),
        autocommit = kw.get('autocommit', True),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
        loop = loop
    )

# 销毁连接池
async def destory_pool():
    global __pool
    if __pool is not None:
        __pool.close()
        await __pool.wait_closed()



# SQL语句的占位符是?，而MySQL的占位符是%s，select()函数在内部自动替换。注意要始终坚持使用带参数的SQL，而不是自己拼接SQL字符串，这样可以防止SQL注入攻击。
# 使用Cursor对象执行select语句时，通过featchall()可以拿到结果集。如果传入size，则拿到指定数量的结果集。结果集是一个list，每个元素都是一个tuple，对应一行记录。
@asyncio.coroutine
def select(sql, args, size = None):
    '''select 语句'''
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield  from cur.fetchall()
        yield from cur.close()
        logging.info('rows retuened: %s' % len(rs))
        return rs

#要执行INSERT、UPDATE、DELETE语句，可以定义一个通用的execute()函数，
# 因为这3种SQL的执行都需要相同的参数，以及返回一个整数表示影响的行数：
@asyncio.coroutine
def execute(sql, args):
    log(sql)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

# 用于输出**元类**中创建sql_insert语句中的占位符
def create_args_string(num):
    l = []
    for n in range(num):
        l.append('?')
    return ','.join(l)


# 定义Field 类， 负责保存（数据库）表的字段名和字段类型
class Field(object):
    def __init__(self, name, colunm_type, primary_key, default):
        self.name = name
        self.colunm_type = colunm_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s, %s>' % (self.__class__.__name__,self.colunm_type, self.name)


# 以下每一种Field分别代表数据库中一种不同的数据属性
class StringField(Field):
    def __init__(self, name = None,  primary_key=False, default=None, ddl='varchar(100)'):
        super(StringField, self).__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name = None, default = False):
        super(BooleanField, self).__init__(name , 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


# 元类
class ModelMetaclass(type):
    '''控制Model对象的创建'''
    def __new__(cls, name, bases, attrs):
        if name =='Model':         # 排除掉对 Model 类的修改
            return type.__new__(cls , name, bases, attrs)
        # 如果没设置 __table__ 属性，tablename 就是类的名字
        tableName = attrs.get('__table__', None) or name
        logging.info('found model :%s (table: %s)' % (name, tableName))
        mappings = {}   # 保存映射关系
        fields = []     # 保存除主键外的属性
        primarykey = None

        # 键是列名，值是field子类
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping: %s ==> %s ' % (k, v))
                # 把键值对存入mapping字典中
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primarykey:
                        raise  Exception('Duplicate primary key for field: %s ' % k)
                    primarykey = k      # 此列设为列表的主键
                else:
                    fields.append(k)     # 保存主键外的属性
        if not primarykey:
            raise Exception('Primary key not found.')
        # 删除类属性
        for k in mappings.keys():
            attrs.pop(k)         #从类属性中删除Field属性,否则，容易造成运行时错误（实例的属性会遮盖类的同名属性）
        # 保存主键外的属性名为‘运算打字符串’列表形式
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


# 基类
class Model(dict, metaclass=ModelMetaclass):    # 在参数处即指定了所依赖的原类
    # 也可在此处写  __metaclass__ = ModelMetaclass，与参数处效果相同
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        # 返回对象的属性,如果没有对应属性则会调用__getattr__
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                # 把默认属性设置进去
                setattr(self, key, value)
        return value


    # 类方法的第一个参数是cls,而实例方法的第一个参数是self
    @classmethod
    @asyncio.coroutine
    def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = yield from select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    @asyncio.coroutine
    def findNumber(cls, selectedField, where=None, args=None):
        ' find number by select and where. '
        # 将列名重命名为_num_
        sql = ['select %s _num_ from `%s`' % (selectedField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        # 限制结果数量为1
        rs = yield from select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    @asyncio.coroutine
    def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = yield from execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    @asyncio.coroutine
    def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = yield from execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)