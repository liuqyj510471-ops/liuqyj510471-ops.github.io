
import pandas as pd
import numpy as np
from datetime import datetime

def sort_bank_transactions(file_path=None, df=None, output_path=None):
    """
    排序银行流水，处理中断情况并标记异常
    
    规则: balance - transactionAmount == 上一条记录的balance
    
    参数:
    file_path: Excel文件路径
    df: DataFrame对象（如果已经加载）
    output_path: 输出文件路径，如果不提供则返回DataFrame
    
    返回:
    sorted_df: 排序后的DataFrame
    """
    
    # 读取数据
    if df is None:
        if file_path is None:
            raise ValueError("必须提供file_path或df参数")
        # 尝试读取不同格式的文件
        if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            raise ValueError("不支持的文件格式，请使用.xlsx, .xls或.csv文件")
    
    # 确保time列是日期时间格式
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
    
    # 创建工作副本
    df_work = df.copy()
    
    # 添加辅助列
    df_work['排序编号'] = 0
    df_work['排序状态'] = '未处理'
    df_work['异常说明'] = ''
    df_work['链编号'] = 0
    df_work['匹配方式'] = ''
    
    # 按时间降序排序（最新的在前）
    df_work = df_work.sort_values(by='time', ascending=False).reset_index(drop=True)
    
    print(f"开始处理银行流水，总记录数: {len(df_work)}")
    print("=" * 60)
    
    # 跟踪已处理的记录
    processed_indices = set()
    current_sort_number = 0
    chain_number = 0
    
    # 第一轮：快速精确匹配排序
    print("\n第一轮：精确匹配排序")
    print("-" * 60)
    
    # 创建余额索引以加速查找
    balance_index = {}
    for idx in df_work.index:
        balance = round(df_work.loc[idx, 'balance'], 2)
        if balance not in balance_index:
            balance_index[balance] = []
        balance_index[balance].append(idx)
    
    while len(processed_indices) < len(df_work):
        # 找到下一个未处理的记录作为新链的起点
        start_idx = None
        for idx in df_work.index:
            if idx not in processed_indices:
                start_idx = idx
                break
        
        if start_idx is None:
            break
        
        # 开始新的排序链
        chain_number += 1
        chain_length = 0
        
        # 标记起始记录
        current_idx = start_idx
        current_sort_number += 1
        df_work.loc[current_idx, '排序编号'] = current_sort_number
        df_work.loc[current_idx, '排序状态'] = '已排序'
        df_work.loc[current_idx, '链编号'] = chain_number
        df_work.loc[current_idx, '匹配方式'] = '链起点'
        processed_indices.add(current_idx)
        chain_length += 1
        
        # 沿着这条链继续排序（只做精确匹配）
        while True:
            current_balance = df_work.loc[current_idx, 'balance']
            current_amount = df_work.loc[current_idx, 'transactionAmount']
            
            # 计算期望的上一笔余额
            expected_prev_balance = round(current_balance - current_amount, 2)
            
            # 使用索引快速查找匹配的余额
            found_match = False
            if expected_prev_balance in balance_index:
                for candidate_idx in balance_index[expected_prev_balance]:
                    if candidate_idx not in processed_indices:
                        # 找到精确匹配
                        current_sort_number += 1
                        df_work.loc[candidate_idx, '排序编号'] = current_sort_number
                        df_work.loc[candidate_idx, '排序状态'] = '已排序'
                        df_work.loc[candidate_idx, '链编号'] = chain_number
                        df_work.loc[candidate_idx, '匹配方式'] = '精确匹配'
                        processed_indices.add(candidate_idx)
                        chain_length += 1
                        
                        current_idx = candidate_idx
                        found_match = True
                        break
            
            if not found_match:
                # 未找到精确匹配，标记链中断
                df_work.loc[current_idx, '异常说明'] = f'链中断：期望上一笔余额为{expected_prev_balance:.2f}'
                break
        
        if chain_length > 1:
            print(f"  链 {chain_number}: {chain_length} 条记录")
    
    print(f"\n第一轮完成，已处理 {len(processed_indices)}/{len(df_work)} 条记录")
    
    # 第二轮：只处理异常记录（链中断点）
    print("\n" + "=" * 60)
    print("第二轮：处理异常记录")
    print("-" * 60)
    
    # 找出所有链中断点
    break_points = df_work[df_work['异常说明'].str.contains('链中断', na=False)].index.tolist()
    
    if break_points:
        print(f"发现 {len(break_points)} 个链中断点，尝试修复...")
        
        # 获取未处理的记录
        unprocessed_indices = set(df_work.index) - processed_indices
        
        # 为未处理记录创建余额索引
        unprocessed_balance_index = {}
        for idx in unprocessed_indices:
            balance = round(df_work.loc[idx, 'balance'], 2)
            if balance not in unprocessed_balance_index:
                unprocessed_balance_index[balance] = []
            unprocessed_balance_index[balance].append(idx)
        
        fixed_count = 0
        
        for break_idx in break_points:
            if df_work.loc[break_idx, '异常说明'].startswith('已修复'):
                continue  # 跳过已修复的
            
            current_balance = df_work.loc[break_idx, 'balance']
            current_amount = df_work.loc[break_idx, 'transactionAmount']
            expected_prev_balance = round(current_balance - current_amount, 2)
            current_chain = df_work.loc[break_idx, '链编号']
            
            # 尝试多种匹配策略
            best_match = None
            best_diff = float('inf')
            match_type = ''
            
            # 策略1: 在±0.5元范围内查找
            for offset in [i * 0.01 for i in range(-50, 51)]:
                test_balance = round(expected_prev_balance + offset, 2)
                if test_balance in unprocessed_balance_index:
                    for idx in unprocessed_balance_index[test_balance]:
                        if idx in unprocessed_indices:
                            diff = abs(offset)
                            if diff < best_diff:
                                best_diff = diff
                                best_match = idx
                                if diff <= 0.01:
                                    match_type = '精确匹配'
                                elif diff <= 0.1:
                                    match_type = '微小差异'
                                else:
                                    match_type = '放宽匹配'
                            break
                if best_match and best_diff <= 0.01:
                    break
            
            # 策略2: 如果还没找到，尝试百分比匹配（可能是金额记录错误）
            if not best_match and abs(expected_prev_balance) > 10:
                for idx in unprocessed_indices:
                    candidate_balance = round(df_work.loc[idx, 'balance'], 2)
                    diff = abs(candidate_balance - expected_prev_balance)
                    percent_diff = diff / abs(expected_prev_balance)
                    
                    if percent_diff <= 0.1 and diff <= 10:  # 10%以内且不超过10元
                        if diff < best_diff:
                            best_diff = diff
                            best_match = idx
                            match_type = '百分比匹配'
            
            if best_match and best_diff <= 0.5:  # 只接受差异在0.5元以内的
                # 插入这条记录
                current_sort_number += 1
                df_work.loc[best_match, '排序编号'] = current_sort_number
                df_work.loc[best_match, '排序状态'] = '已排序-二次插入'
                df_work.loc[best_match, '链编号'] = current_chain
                df_work.loc[best_match, '匹配方式'] = f'{match_type}(差异{best_diff:.2f})'
                df_work.loc[best_match, '异常说明'] = f'二次插入：期望{expected_prev_balance:.2f}，实际{df_work.loc[best_match, "balance"]:.2f}'
                processed_indices.add(best_match)
                unprocessed_indices.discard(best_match)
                
                # 从索引中移除
                actual_balance = round(df_work.loc[best_match, 'balance'], 2)
                if actual_balance in unprocessed_balance_index:
                    unprocessed_balance_index[actual_balance].remove(best_match)
                
                # 更新原中断点的异常说明
                df_work.loc[break_idx, '异常说明'] = f'已修复：通过{match_type}找到后续记录(差异{best_diff:.2f})'
                
                fixed_count += 1
                print(f"  ✓ 修复链 {current_chain}: ID={df_work.loc[break_idx, 'id']} -> "
                      f"ID={df_work.loc[best_match, 'id']} ({match_type}, 差异{best_diff:.2f}元)")
                
                # 尝试继续延伸这条链
                current_idx = best_match
                extended = 0
                
                while True:
                    current_balance = df_work.loc[current_idx, 'balance']
                    current_amount = df_work.loc[current_idx, 'transactionAmount']
                    expected_prev_balance = round(current_balance - current_amount, 2)
                    
                    # 先尝试精确匹配
                    found_match = False
                    if expected_prev_balance in unprocessed_balance_index:
                        for candidate_idx in unprocessed_balance_index[expected_prev_balance]:
                            if candidate_idx in unprocessed_indices:
                                current_sort_number += 1
                                df_work.loc[candidate_idx, '排序编号'] = current_sort_number
                                df_work.loc[candidate_idx, '排序状态'] = '已排序-延伸'
                                df_work.loc[candidate_idx, '链编号'] = current_chain
                                df_work.loc[candidate_idx, '匹配方式'] = '延伸精确匹配'
                                df_work.loc[candidate_idx, '异常说明'] = ''
                                processed_indices.add(candidate_idx)
                                unprocessed_indices.discard(candidate_idx)
                                
                                # 从索引中移除
                                actual_balance = round(df_work.loc[candidate_idx, 'balance'], 2)
                                if actual_balance in unprocessed_balance_index:
                                    unprocessed_balance_index[actual_balance].remove(candidate_idx)
                                
                                current_idx = candidate_idx
                                extended += 1
                                found_match = True
                                break
                    
                    if not found_match:
                        # 再尝试放宽匹配
                        for offset in [i * 0.01 for i in range(-50, 51)]:
                            test_balance = round(expected_prev_balance + offset, 2)
                            if test_balance in unprocessed_balance_index:
                                for candidate_idx in unprocessed_balance_index[test_balance]:
                                    if candidate_idx in unprocessed_indices:
                                        diff = abs(offset)
                                        if diff <= 0.5:  # 只接受0.5元以内的差异
                                            current_sort_number += 1
                                            df_work.loc[candidate_idx, '排序编号'] = current_sort_number
                                            df_work.loc[candidate_idx, '排序状态'] = '已排序-延伸'
                                            df_work.loc[candidate_idx, '链编号'] = current_chain
                                            df_work.loc[candidate_idx, '匹配方式'] = f'延伸放宽匹配(差异{diff:.2f})'
                                            df_work.loc[candidate_idx, '异常说明'] = ''
                                            processed_indices.add(candidate_idx)
                                            unprocessed_indices.discard(candidate_idx)
                                            
                                            # 从索引中移除
                                            actual_balance = round(df_work.loc[candidate_idx, 'balance'], 2)
                                            if actual_balance in unprocessed_balance_index:
                                                unprocessed_balance_index[actual_balance].remove(candidate_idx)
                                            
                                            current_idx = candidate_idx
                                            extended += 1
                                            found_match = True
                                            break
                            if found_match:
                                break
                    
                    if not found_match:
                        break
                
                if extended > 0:
                    print(f"    └─ 延伸了 {extended} 条记录")
        
        print(f"\n第二轮完成，修复了 {fixed_count} 个中断点")
    else:
        print("没有发现链中断点")
    
    # 第三轮：处理剩余的孤立记录
    remaining = set(df_work.index) - processed_indices
    if remaining:
        print("\n" + "=" * 60)
        print(f"第三轮：处理 {len(remaining)} 条剩余记录")
        print("-" * 60)
        
        # 尝试将剩余记录插入到现有链的间隙中
        for remain_idx in remaining:
            remain_balance = df_work.loc[remain_idx, 'balance']
            remain_amount = df_work.loc[remain_idx, 'transactionAmount']
            
            # 计算这条记录的前后期望值
            expected_prev = round(remain_balance - remain_amount, 2)
            expected_next = remain_balance
            
            # 在已排序的记录中查找可能的插入位置
            best_insert_pos = None
            best_score = float('inf')
            
            for idx in processed_indices:
                idx_balance = df_work.loc[idx, 'balance']
                idx_amount = df_work.loc[idx, 'transactionAmount']
                idx_expected_prev = round(idx_balance - idx_amount, 2)
                
                # 检查是否可以插入到这条记录之前
                # 即：remain_balance ≈ idx_expected_prev
                diff1 = abs(remain_balance - idx_expected_prev)
                
                # 同时检查前一条记录
                idx_sort_num = df_work.loc[idx, '排序编号']
                prev_records = df_work[df_work['排序编号'] == idx_sort_num - 1]
                
                if len(prev_records) > 0:
                    prev_idx = prev_records.index[0]
                    prev_balance = df_work.loc[prev_idx, 'balance']
                    diff2 = abs(expected_prev - prev_balance)
                    
                    total_diff = diff1 + diff2
                    if total_diff < best_score and total_diff < 1.0:
                        best_score = total_diff
                        best_insert_pos = idx
            
            if best_insert_pos is not None:
                # 找到插入位置，重新分配排序编号
                insert_sort_num = df_work.loc[best_insert_pos, '排序编号']
                
                # 更新该位置之后的所有记录排序编号
                for idx in df_work.index:
                    if df_work.loc[idx, '排序编号'] >= insert_sort_num:
                        df_work.loc[idx, '排序编号'] += 1
                
                # 插入新记录
                df_work.loc[remain_idx, '排序编号'] = insert_sort_num
                df_work.loc[remain_idx, '排序状态'] = '已排序-插入'
                df_work.loc[remain_idx, '链编号'] = df_work.loc[best_insert_pos, '链编号']
                df_work.loc[remain_idx, '匹配方式'] = '插入匹配'
                df_work.loc[remain_idx, '异常说明'] = f'插入到链{df_work.loc[best_insert_pos, "链编号"]}中'
                
                print(f"  ✓ 插入记录: ID={df_work.loc[remain_idx, 'id']} 到排序编号 {insert_sort_num}")
            else:
                # 如果无法插入，标记为孤立记录
                current_sort_number += 1
                df_work.loc[remain_idx, '排序编号'] = current_sort_number
                df_work.loc[remain_idx, '排序状态'] = '孤立记录'
                df_work.loc[remain_idx, '异常说明'] = '无法找到合适的插入位置'
                df_work.loc[remain_idx, '链编号'] = 0
                print(f"  ⚠ 孤立记录: ID={df_work.loc[remain_idx, 'id']} 无法插入任何链")
    
    # 按排序编号排序
    result_df = df_work.sort_values(by='排序编号').reset_index(drop=True)
    
    # 验证排序结果
    print("\n" + "=" * 60)
    print("排序验证:")
    error_count = 0
    for i in range(len(result_df) - 1):
        current_balance = result_df.loc[i, 'balance']
        current_amount = result_df.loc[i, 'transactionAmount']
        next_balance = result_df.loc[i + 1, 'balance']
        
        expected_next = round(current_balance - current_amount, 2)
        actual_next = round(next_balance, 2)
        
        # 只在同一链内验证
        if result_df.loc[i, '链编号'] == result_df.loc[i + 1, '链编号']:
            if abs(expected_next - actual_next) > 0.01:
                error_count += 1
                if error_count <= 5:  # 只显示前5个错误
                    print(f"  验证失败 [行{i}->行{i+1}]: "
                          f"期望={expected_next}, 实际={actual_next}, "
                          f"差异={abs(expected_next - actual_next):.2f}")
    
    if error_count == 0:
        print("  ✓ 所有记录验证通过")
    else:
        print(f"  ✗ 发现 {error_count} 处验证失败")
    
    # 统计信息
    print("\n" + "=" * 60)
    print("排序统计:")
    print(f"  总记录数: {len(result_df)}")
    print(f"  排序链数量: {chain_number}")
    print(f"  正常排序: {len(result_df[result_df['排序状态'] == '已排序'])}")
    print(f"  异常记录: {len(result_df[result_df['异常说明'] != ''])}")
    
    # 显示异常记录
    abnormal_df = result_df[result_df['异常说明'] != '']
    if len(abnormal_df) > 0:
        print("\n异常记录详情:")
        for idx, row in abnormal_df.iterrows():
            print(f"  行{idx}: ID={row['id']}, {row['异常说明']}")
    
    # 保存结果
    if output_path:
        if output_path.endswith('.xlsx'):
            result_df.to_excel(output_path, index=False)
        elif output_path.endswith('.csv'):
            result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n结果已保存到: {output_path}")
    
    return result_df


def analyze_transaction_chains(df):
    """
    分析排序后的交易链，提供详细的链信息
    
    参数:
    df: 排序后的DataFrame
    
    返回:
    chain_info: 包含每条链信息的DataFrame
    """
    if '链编号' not in df.columns:
        print("请先运行sort_bank_transactions进行排序")
        return None
    
    chain_info = []
    
    for chain_num in df['链编号'].unique():
        if chain_num == 0:
            continue
        
        chain_df = df[df['链编号'] == chain_num]
        
        info = {
            '链编号': chain_num,
            '记录数': len(chain_df),
            '起始时间': chain_df['time'].max(),
            '结束时间': chain_df['time'].min(),
            '起始余额': chain_df.iloc[0]['balance'],
            '结束余额': chain_df.iloc[-1]['balance'],
            '交易总额': chain_df['transactionAmount'].sum(),
            '是否有异常': (chain_df['异常说明'] != '').any()
        }
        
        chain_info.append(info)
    
    chain_info_df = pd.DataFrame(chain_info)
    return chain_info_df


# 使用示例
if __name__ == "__main__":
    # 示例1: 从文件读取并排序
    input_file = r"INPUT_FILE.xlsx" # 修改为你的文件路径
    output_file = r"INPUT_FILE.xlsx"
    
    try:
        # 执行排序
        sorted_df = sort_bank_transactions(
            file_path=input_file,
            output_path=output_file
        )
        
        # 分析排序链
        print("\n" + "=" * 60)
        print("排序链分析:")
        chain_info = analyze_transaction_chains(sorted_df)
        if chain_info is not None:
            print(chain_info.to_string(index=False))
        
    except FileNotFoundError:
        print(f"错误：找不到文件 {input_file}")
        print("请修改input_file变量为你的实际文件路径")
    except Exception as e:
        print(f"处理过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()
