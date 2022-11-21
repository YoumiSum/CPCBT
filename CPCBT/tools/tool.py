import copy
import warnings
from functools import wraps
from inspect import getfullargspec
from collections.abc import Iterable


def paramcheck(func):
    @wraps(func)
    def inner(*args, **kwargs):
        func_args = getfullargspec(func)
        youmi = dict(kwargs)
        youmi.update({func_args[0][index]: args[index] for index in range(len(args))})

        annotations = dict(func_args[-1])
        for key in annotations.keys():
            if key == "return":
                continue
            if youmi.get(key) is None:
                continue

            # 增加int到float的自动转换
            if annotations.get(key) == float and isinstance(youmi.get(key), int):
                youmi.update({key: float(youmi.get(key))})

            assert issubclass(type(youmi.get(key)), annotations.get(key)), f"\n{key} must be {annotations.get(key)}, but got ({youmi.get(key)}, {type(youmi.get(key))})"

        del youmi
        del annotations

        return func(*args, **kwargs)

    return inner


def deserted(func):

    @wraps(func)
    def inner(*args, **kwargs):
        warnings.warn(f"WARNING: this function({str(func).split(' ')[1]}) will be delete in future", FutureWarning, stacklevel=2)
        return func(*args, **kwargs)
    return inner


@paramcheck
def muti_for_simula(lengths: Iterable = []):
    """
    多层 for 循环模拟器，当你无法确定需要动态的生成for循环套用的次数的时候，可以使用它
    :param lengths: 每个index所对应的长度length
    :return: 生成的索引列表
    """
    indexes = [0 for i in lengths]
    lengths = lengths[::-1]

    # 下面的操作将模拟加法的运算过程
    flag = 0        # 进位标志符
    while True:
        yield list(indexes[::-1])
        for i, item in enumerate(lengths):
            indexes[i] += flag
            # 对于第一个数字，需要单独的处理，因为它需要进行自增
            if i == 0:
                if indexes[i] >= item - 1:
                    if i == len(indexes) - 1:
                        return None

                    flag = 1
                    indexes[i] = -1
                else:
                    flag = 0

            else:
                if indexes[i] >= item:
                    if i == len(indexes) - 1:
                        return None

                    flag = 1
                    indexes[i] = 0
                else:
                    flag = 0

        indexes[0] += 1


def unpack_lst(lst):
    """
    将lst列表给解压出来
    :param lst:
    :return:
    """
    if isinstance(lst, list):
        for item in lst:
            yield from unpack_lst(item)
    else:
        yield lst

@paramcheck
def gen_step(num: int, n: int) -> list:
    """
    解决以下问题：
        一段长度为length的路程，分step步走，总共有多少种走法，分布都是什么
    返回值：步骤集合
    :param length:
    :param step:
    :return:
    """

    lst_com = []
    if n == 1:
        return [[num]]

    else:
        for i in range(num, -1, -1):
            this = i
            residue = num - i
            lst = gen_step(residue, n - 1)
            for item in lst:
                item.insert(0, this)

            for item in lst:
                lst_com.append(item)
        return lst_com


class Singleton(type):
    # new用于创建类对象
    def __new__(cls, class_name, class_parents, class_attr):
        # 给类添加一个instance属性
        class_attr['instance'] = None

        # 返回类对象
        return super(Singleton, cls).__new__(cls, class_name, class_parents, class_attr)

    # 类对象实例化时将调用
    def __call__(self, *args, **kwargs):
        if self.instance is None:
            self.instance = super(Singleton, self).__call__(*args, **kwargs)
        return self.instance


def dict_deep_update_nobreak(dict1: dict, dict2: dict):
    """
    字典深度更新（不破坏原始数据）
    :param dict1:
    :param dict2:
    :return:
    """
    res_dict = dict(dict1)
    dict_deep_update(res_dict, dict2)
    return res_dict


def dict_deep_update(dict1: dict, dict2: dict):
    """
    字典深度更新
    :param dict1:   需要被更新的字典
    :param dict2:   更新的内容
    :return:
    """
    dict_now = dict1
    for key in dict2.keys():
        try:
            item = dict_now.get(key)
            if isinstance(item, dict):
                dict_deep_update(item, dict2.get(key))

            else:
                dict_now.update({key, dict2.get(key)})

        except:
            # 如果当前所在节点没有该属性，则直接更新
            dict_now.update({key: dict2.get(key)})


if __name__ == '__main__':
    def dict_deep_update():
        dit = {"a": 1, "b": {"b1": 1}}
        dit2 = {"a": 2, "c": 2, "b": {"b1": 2, "b2": {"lst": [1, 2, 4]}}}

        dict_deep_update(dit, dit2)
        print(dit)

    def gen_step_Test():
        """
        生成指定走路步骤，具体看结果
        :return:
        """
        for item in gen_step(5, 3):
            print(item)
        """
            [5, 0, 0]
            [4, 1, 0]
            [4, 0, 1]
            [3, 2, 0]
            [3, 1, 1]
            [3, 0, 2]
            [2, 3, 0]
            [2, 2, 1]
            [2, 1, 2]
            [2, 0, 3]
            [1, 4, 0]
            [1, 3, 1]
            [1, 2, 2]
            [1, 1, 3]
            [1, 0, 4]
            [0, 5, 0]
            [0, 4, 1]
            [0, 3, 2]
            [0, 2, 3]
            [0, 1, 4]
            [0, 0, 5]
        """


    def paramcheckTest():
        """
            使用示例：如下列所示：self和right都没有进行校验，因为其没有加 :类型
                而其余有加的都会进行验证
        """
        class Test(object):

            @paramcheck
            def __init__(self, name: str = None, left: float = -1.0, right=-1.0):
                print(left)
                print(right)

            @deserted
            def del_test(self):
                print("AA")

        Test()

    paramcheckTest()

    def muti_for_simula_Test():
        """
            for循环模拟器
        """
        for item in muti_for_simula([3, 3]):
            print(item)
            # 结果如下：
            #     [0, 0]
            #     [0, 1]
            #     [0, 2]
            #     [1, 0]
            #     [1, 1]
            #     [1, 2]
            #     [2, 0]
            #     [2, 1]
            #     [2, 2]
            #
        print("-------------------")
        for item in muti_for_simula([3, 0]):
            print(item)
            # 结果如下
            #     [0, 0]
            #     [1, 0]
            #     [2, 0]


    def sigle_test():
        class Test(metaclass=Singleton):
            def __init__(self, id):
                self.id = id

        t = Test(1)
        t1 = Test(2)
        print(t.id)     # 1
        print(t1.id)    # 1
        print(t is t1)




