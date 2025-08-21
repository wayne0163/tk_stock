def to_ts_code(code: str) -> str:
    """将6位股票代码转换为Tushare的ts_code格式"""
    if not isinstance(code, str):
        code = str(code)
    
    code = code.strip()
    
    # 检查是否已经是ts_code格式
    if code.endswith(('.SH', '.SZ', '.BJ')):
        return code
    
    # 检查是否是6位数字
    if len(code) == 6 and code.isdigit():
        if code.startswith('6'):
            return f"{code}.SH"
        elif code.startswith('8'):
            return f"{code}.BJ"
        else:
            return f"{code}.SZ"
    
    # 如果格式不正确，返回原始代码，让上层处理
    return code