import os
import re
import sys
import fitz
import json
import time
import psutil
import requests
import pythoncom
import traceback
import win32com.client
from pygments.lexer import words
from numcheck_azure_operation import AzureOperation

azure_opt = AzureOperation()

EXTRACT_PROMPT = f"""
帮我提取图片中的正文部分。
图片是由其对应的 pdf 文件另存来的，我会给你图片和从 pdf 里复制出来的文字, 帮我提取图片中的正文部分
请参考图片，做以下的处理
1. 页眉与页脚的内容不需要提取 (页脚内容是指下方的公司信息，以及其上方由两个横线隔开的区域里，字体和字体大小明显和正文不一样的内容)
2. 图片上看,被大面积有颜色的方框遮盖的内容不需要提取
3. 大标题与小标题不需要提取
4. 根据以上要点，解析图片内容. 区分页眉、页脚和正文
4. 没有被有颜色的方框遮盖的正文可能包含若干小表格，如果有，这部分表格数据整理成 markdown 表格格式的文本
将正文的内容，以段落形式输出，每段内容之间用换行符分隔
要回答原文内容, 包括段落开头的符号等，不要有任何修改，并且不要回答其他内容。
图片文字内容如下:
====================
"""

CIRCLE_CHECK_PROMPT = """
帮我筛选出符号列表里的符号是否都是常见的无序列表的符号，比如【圆圈、双圆圈、实心圆圈、菱形、实心菱形、方块、实心方块、星形、实心星形】
以json格式返回结果, 比如：
如果符号列表里没有符合需求的符号，则返回 {{"result": “no_circle”}}
如果符号列表里有符合需求的符号，则返回数量最多的那个符号，{{"result": "【符号】"}}
符号列表：【{circles}】
"""

# 股票类型
PUBLIC_FUND_TYPE = "public"
PRIVATE_FUND_TYPE = "private"

# 提取序号符号正则表达式
CIRCLE_REX = r"(?m)^([^ \u4E00-\u9FFF\w\d\n\u3040-\u30FF])([ \u3000]?)(.*)"
# 提取文本空格的正则表达式
SPACE_REX = r"(?<![{index_symbols}\u4E00-\u9FFFa-zA-Z1-9 \u3000\xa0])[ \u3000\xa0]{{1,2}}(?![a-zA-Z \u3000\xa0]).{{,15}}"

# 文本匹配度
MATCH_RATE = 0.5

# vba 宏文件
VBA_EXCEL_FILE = r"D:\projects\test\read_excel_pic\vba_tool.xlsm"

# 文件名的股票编码以及对应要检查的 sheet 页列表
CODE_SHEET_NAME_MAP = {
    "140153": ["※実質投資配分のコメント"],
    "140653": ["配分変更コースのコメント"],
    "140672": ["※①②コメント "],
    "140687": ["コメント"],
    "140779-82": ["※社債銘柄紹介", "※新興国銘柄紹介", "※小型株銘柄紹介", "※REIT銘柄紹介"],
    "140793-6": ["※貼付コメント(株)", "※貼付コメント(債券)", ""],
    "180168-73": ["コメント"],
    "180190-5アジア通貨セレクトコース": ["アジア通貨セレクト"],
    "300241": ["コメント"],
    "300424-425": ["月報フォーマット（日本語）core1"],
    "400049": ["DC_ネクスト10"],
    "400052": ["DC_運用戦略M"],
    "63011": ["コメント"],
    "63207": ["005"],
    "64169": ["ポートフォリオ配分コメント"],
    "通貨セレクトコース": ["通貨セレクト "],
}

# ai 调用相关
API_URL_DEV = "https://namcheckwebrpa-uat.azurewebsites.net/api/call_openai_with_global_lock"
proxy_host = "172.19.248.1:92"  # 替换为代理地址和端口
proxy_username = "80023"  # 替换为代理的用户名
proxy_password = "nomura55"  # 替换为代理的密码
proxy_with_auth = f"http://{proxy_username}:{proxy_password}@{proxy_host}"

os.environ["HTTP_PROXY"] = proxy_with_auth
os.environ["HTTPS_PROXY"] = proxy_with_auth

def get_ai_response(messages, image_path=None):
    """
    调用AI模型API，可以处理文本和可选的图片输入，并返回AI的回答。

    :param messages: 一个包含对话消息的列表。
    :param image_path: (可选) 一个指向图片文件的字符串路径。
    :return: AI返回的回答内容字符串，如果出错则返回错误信息。
    """

    # 如果不需要上传图片
    if not image_path:
        ai_content = azure_opt.call_openai_messageonly(messages)
        if not ai_content:
            return "Error: call_openai_messageonly returned empty or invalid response."
        print(f"   -> 📡 HTTP Status: 200 (local-call)") 
        return ai_content
    resp = azure_opt.call_openai_with_image_file(messages, image_path)
    if not resp or not isinstance(resp, dict):
        print("   -> ❌ Error: call_openai_with_image_file returned invalid response")
        return "Error: call_openai_with_image_file returned empty or invalid response."

    # 取 content（与你文本模式一致）
    try:
        choices = resp.get("choices", [])
        ai_content = None
        for choice in choices:
            msg = choice.get("message")
            if isinstance(msg, dict):
                content = msg.get("content")
                if isinstance(content, str) and content.strip():
                    ai_content = content
                    break

        if not ai_content:
            print("   -> ❌ Error: no content in choices")
            return "Error: no content returned."

    except Exception as e:
        print(f"   -> ❌ Error parsing response: {e}")
        return "Error: failed to parse response."

    print(f"   -> 📡 HTTP Status: 200 (local-call)")
    return ai_content


def ai_check_circle(circles:str, prompt:str):
    if not circles:
        return ""
    try:
        final_prompt = prompt.format(circles=circles)
        messages = [
            {"role": "system", "content": "你是一名专业的文档处理人员，能精确处理用户需求"},
            {"role": "user", "content": final_prompt}
        ]

        ai_result = get_ai_response(messages)
        return ai_result

    except Exception as e:
        traceback.print_exc()
        print(f"   -> error - ai_check_circle: {e}")
        return ""


def get_ai_insight(all_text, prompt: str, png_path):
    """
    处理一个Excel文件，将其转换为图片和文本，然后调用AI进行分析。
    【已更新】使用 PyMuPDF (fitz) 替代 pdf2image 和 pdfplumber。

    :param all_text: 所有文本
    :param prompt: 发送给AI的固定提示词，提取的文本将附加在此提示后。
    :param png_path: 页面图片
    :return: AI的分析结果。
    """

    try:

        final_prompt = f"{prompt}\n[{all_text}]"

        messages = [
            {"role": "system", "content": "你是一名专业的文档处理人员，能精确处理用户需求"},
            {"role": "user", "content": final_prompt}
        ]

        ai_result = get_ai_response(messages, image_path=png_path)
        return ai_result

    except Exception as e:
        traceback.print_exc()
        print(f"   -> error - process_excel_and_get_ai_insight: {e}")
        return ""


def is_text_match(text: str, article_text: str):
    """
    判断文本是否在正文文本内
    Args:
        text: 文本
        article_text: 正文文本

    Returns:
    """
    try:
        # 拆分 text
        text_list = text.split("\n")
        cnt = 0
        total = 0
        for text in text_list:
            if not text:
                continue
            if text.strip().strip('。').strip() in article_text:
                cnt += 1
            total += 1

        # if total == 0:
        #     print(f"   -> text {text[:100]} not in article text, match rate: {0.0}")
        #     return False

        if cnt / total >= MATCH_RATE:   # 判断阈值
            print(f"   -> text {text[:100]} in article text, match rate: {round(cnt/total, 4)}")
            return True
        else:
            print(f"   -> text {text[:100]} not in article text, match rate: {round(cnt/total, 4)}")
            return False
    except Exception as e:
        traceback.print_exc()
        print(f"   -> error - is_text_match - text {text} - {e}")

def get_file_code_and_month(file_path: str):
    """
    获取文件的股票编号和月份
    Args:
        file_path:

    Returns:
    """
    try:
        base_name = os.path.basename(file_path)
        file_name = os.path.splitext(base_name)[0]

        file_code = ""
        base_month = ""
        if file_name:
            file_code = file_name.split("_")[0]
            base_month = file_name.split("_")[1]
        return dict(
            file_code=file_code,
            base_month=base_month,
        )
    except Exception as e:
        traceback.print_exc()
        print(f"   -> error: 无法获取到股票编码")
        return {}


def resource_path(relative_path):
    """
    获取资源的绝对路径。
    无论是在开发环境中运行脚本，还是在打包后的EXE中运行，都能找到正确路径。
    """
    try:
        # PyInstaller 会创建一个临时文件夹，并将路径存储在 _MEIPASS 中
        base_path = sys._MEIPASS
    except Exception as e:
        # 如果不在打包环境中，则使用当前脚本所在的目录
        traceback.print_exc()
        base_path = os.path.abspath(".")
        print(f"resource_path:{relative_path}, error: {e}")
        return ""

    return os.path.join(base_path, relative_path)


class ExcelProcessor:
    """
    一个用于处理单个Excel文件的类。
    它负责提取内容、检查格式问题（空格、颜色、字体等）并将Excel转换为PDF。
    """
    def __init__(self):
        pass

    # --- 进程清理函数 ---
    @staticmethod
    def cleanup_office_processes():
        """
        在程序启动时，强制关闭所有残留的Excel和Word进程，以防止自动化冲突。
        """
        print("   -> --- 准备环境: 正在检查并清理残留的 Office 进程... ---")
        # 需要清理的进程名称列表
        processes_to_kill = ["EXCEL.EXE", "WINWORD.EXE"]

        killed_count = 0
        for proc in psutil.process_iter(['pid', 'name']):
            # 检查进程名称是否在我们的目标列表中
            if proc.info['name'] in processes_to_kill:
                try:
                    p = psutil.Process(proc.info['pid'])
                    p.kill()  # 强制终止进程
                    print(
                        f"   -> 发现并强制关闭了一个残留进程: {proc.info['name']} (PID: {proc.info['pid']})")
                    killed_count += 1
                except psutil.NoSuchProcess:
                    # 进程在检查和终止之间已经消失，是正常情况
                    pass
                except Exception as e:
                    print(f"   -> 尝试关闭进程 {proc.info['name']} (PID: {proc.info['pid']}) 时出错: {e}")

        if killed_count == 0:
            print("   -> --- 环境干净，未发现残留的 Office 进程。 ---")
        else:
            print(f"   -> --- 环境清理完毕，共关闭了 {killed_count} 个残留进程。 ---")

        @staticmethod
        def wait_for_file_access(file_path, timeout=5):
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    with open(file_path, "rb"):
                        return True
                except IOError:
                    time.sleep(0.5)
            return False

    @staticmethod
    def _get_covered_cell_addresses(sheet):
        """
        获取单个工作表上被所有图形完全或部分覆盖的单元格地址集合。

        Args:
            sheet (win32com.client.CDispatch): 要分析的 Excel 工作表对象。

        Returns:
            set: 一个包含所有被覆盖单元格地址（例如 '$A$1', '$B$2'）的集合。
        """
        covered_cells = set()
        for shape in sheet.Shapes:
            try:
                # 使用 TopLeftCell 和 BottomRightCell 属性直接获取覆盖范围
                # 这是比计算宽高更可靠的方法
                top_left = shape.TopLeftCell
                bottom_right = shape.BottomRightCell

                # 创建一个 Range 对象
                shape_range = sheet.Range(top_left, bottom_right)

                # 遍历该范围内的所有单元格，并将其地址添加到集合中
                for cell in shape_range.Cells:
                    covered_cells.add(cell.Address)
            except Exception:
                # 某些特殊形状可能没有这些属性，为避免程序崩溃，我们忽略它们
                continue
        return covered_cells

    def get_space(self, text, page, fund_type, global_standard_pattern, index_circle):
        """
        检查空格
        Args:
            text: 单元格/文本框的文本
            page: 页码d
            fund_type: 股票类型
            global_standard_pattern: 序号符号后的空格标准
            index_circle: 正文文本里的序号符号

        Returns:
        """
        # 检查行首缩进
        start_space = self.get_line_start_space(text, page)

        # 提取序号符号后的空格标准
        if global_standard_pattern:
            standard_pattern = global_standard_pattern
        else:
            print(f"   -> error: get_space - no global_standard_pattern")
            return [], text

        circle_result = []
        effective_text = text
        circle_matches = list(re.finditer(CIRCLE_REX, effective_text))
        for match in circle_matches:
            space_after = match.group(2)
            current_pattern = "circle_no_space"
            if space_after == "\u3000":
                current_pattern = "circle_full_space"
            elif space_after == " ":
                current_pattern = "circle_half_space"

            if current_pattern != standard_pattern:
                start_pos, end_pos = match.start(), min(match.end() + 5, len(effective_text))
                context = effective_text[start_pos:end_pos]
                circle_result.append(
                    {'original_text': context.replace(" ", "").replace("\u3000", "").replace("\xa0", ""), 'page': page,
                     'reason_type': "同一表現"})

        index_circle = re.escape(index_circle)
        space_list = re.findall(SPACE_REX.format(index_symbols=index_circle), text)

        res_dict = [
            *list(map(lambda x: dict(
                original_text=re.sub(r"(?<![0-9A-Za-z])[ \u3000\xa0]", "", x) if re.search(r"\d", x)
                                        else x.replace(" ", "").replace( "\u3000", "").replace("\xa0", ""),
                page=page,
                reason_type="不自然な空白"
            ), space_list)),
            *circle_result
        ]
        _res_dict = list(filter(lambda x: len(x["original_text"]) > 1, res_dict))
        _res_dict.extend(start_space)
        return _res_dict, text

    @staticmethod
    def get_line_start_space(text, page):
        """
        检查行首缩进
        Args:
            text:
            page:

        Returns:
        """
        line_filter = re.findall(r"^[ \u3000\xa0]{0,2}.{0,20}", text, flags=re.MULTILINE)
        if len(line_filter) <= 1: return []
        line_list = list(filter(lambda x: x != "", line_filter))
        line_dict, line_marks = {}, []
        for once in line_list:
            m = re.match(r"^([ \u3000\xa0]{0,2})", once)
            space_len = len(m.group(1)) if m else 0
            mark = '⁂' * space_len
            line_marks.append(mark)
            line_dict[mark] = line_dict.get(mark, 0) + 1
        standard = ""
        if line_dict and len(line_dict.keys()) != 1:
            standard = line_marks[0] if len(set(line_dict.values())) == 1 else max(line_dict, key=line_dict.get)
        error_lines = []
        for once in line_list:
            m = re.match(r"^([ \u3000\xa0]{0,2})", once)
            space_len = len(m.group(1)) if m else 0
            mark = '⁂' * space_len
            if mark != standard: error_lines.append(once)
        res_dict = list(
            map(lambda x: dict(original_text=x.replace(" ", "").replace("\u3000", "").replace("\xa0", ""), page=page,
                               reason_type="行首缩进-同一表現"), error_lines))
        return list(filter(lambda x: len(x["original_text"]) > 1, res_dict))

    @staticmethod
    def is_allowed_color(ole_color):
        """
        判断是否是正常的颜色
        Args:
            ole_color:

        Returns:
        """
        if ole_color is None: return True
        color_val = int(ole_color)
        try:
            r = color_val & 0xFF
            g = (color_val >> 8) & 0xFF
            b = (color_val >> 16) & 0xFF
            if r < 30 and g < 30 and b < 30:
                return True
            else:
                return False
        except Exception as e:
            if color_val == -4105: return True
            return False


    def extract_data_from_sheets(self, workbook, excel, excel_path, fund_type, dataset, debug_mode=False):
        """
        提取 sheet 页的所有 单元格/文本框 等 shape 的数据
        Args:
            workbook:
            excel:
            excel_path:
            fund_type:
            dataset:
            debug_mode:

        Returns:

        """
        print(f"   -> processing extract_data_from_sheets ...")
        all_content_data = []
        page_counter = 0

        # 只处理规定范围内的 sheet
        code_month = get_file_code_and_month(excel_path)
        file_code = code_month.get("file_code")

        for sheet in workbook.Worksheets:
            covered_cell_addresses = self._get_covered_cell_addresses(sheet)

            shape_list = [s for s in sheet.Shapes]
            for shape in shape_list:
                if shape.Type in [1, 17]:
                    # 文本数据
                    text = shape.TextFrame.Characters().Text
                    _text = text.replace(" ", "").replace("\u3000", "")
                    if not text: continue
                    if not _text: continue

                    try:
                        # 这段代码是为了解决某些渲染问题，如果失败，我们只记录警告而不中断程序
                        if shape.Height - shape.TextFrame2.TextRange.BoundHeight < 7 and shape.TextFrame.Characters().Font.Size > 15:
                            shape.TextFrame.Characters().Font.Size -= 5
                        else:
                            # 导致错误的就是下面这行，尤其是在字体大小已经是1的时候
                            if shape.TextFrame.Characters().Font.Size > 1:
                                shape.TextFrame.Characters().Font.Size -= 1
                    except Exception as e:
                        print(
                            f"   -> error: can't modify shape font size, sheet: {sheet.Name}, continue")
                        pass  # 跳过这个操作，继续执行

                    # todo: 同一个单元格/文本框里的文本有多处颜色错误的文本，会被拼接在一起（abnormal_color_chars）, 是否需要拆分
                    rich_text_info, abnormal_color_chars = [], ""
                    for i in range(1, len(text) + 1):
                        char_obj = shape.TextFrame.Characters(i, 1)
                        char_text, char_color = char_obj.Text, char_obj.Font.Color
                        rich_text_info.append({'char': char_text, 'color': char_color})
                        if not self.is_allowed_color(char_color):
                            abnormal_color_chars += char_text

                    if abnormal_color_chars == "\n":
                        # 忽略换行符
                        continue

                    shape_data = {
                        "source": "shape", "page": page_counter, "sheet_name": sheet.Name, "text": text,
                        "top": shape.Top, "left": shape.Left, "font_name": shape.TextFrame2.TextRange.Font.Name,
                        "font_bold": shape.TextFrame2.TextRange.Font.Bold, "rich_text_info": rich_text_info,
                        "abnormal_color_text": abnormal_color_chars
                    }
                    all_content_data.append(shape_data)

            # 单元格值
            used_range = sheet.UsedRange
            cell_values = used_range.Value
            if cell_values:
                if not isinstance(cell_values, (list, tuple)):
                    cell_values = [[cell_values]]
                elif len(cell_values) > 0 and not isinstance(cell_values[0], (list, tuple)):
                    cell_values = [cell_values]
                start_row, start_col = used_range.Row, used_range.Column
                for r_idx, row_data in enumerate(cell_values):
                    if not isinstance(row_data, (list, tuple)): row_data = [row_data]
                    for c_idx, cell_value in enumerate(row_data):
                        if cell_value:
                            text = str(cell_value)
                            abs_row, abs_col = start_row + r_idx, start_col + c_idx
                            cell_obj = sheet.Cells(abs_row, abs_col)

                            is_covered = cell_obj.Address in covered_cell_addresses

                            font = cell_obj.Font
                            cell_data = {
                                "source": "cell", "page": page_counter, "sheet_name": sheet.Name, "text": text,
                                "row": abs_row, "col": abs_col, "font_name": font.Name, "font_bold": font.Bold,
                                "font_color": font.Color,
                                "is_covered": is_covered
                            }
                            all_content_data.append(cell_data)

            # 筛选出在规定 sheet 页内的数据
            is_data_sheet = False

            # 如果文件名没有 code，则默认处理
            if not file_code:
                print(f"   -> excel_path: {excel_path}, 无法识别到 code, 默认进行处理")
                is_data_sheet = True

            if fund_type == PUBLIC_FUND_TYPE:
                # 公募数据按照 sheet 列表筛选
                allowed_sheet_names = CODE_SHEET_NAME_MAP.get(file_code, [])
                if allowed_sheet_names:
                    is_data_sheet = sheet.Name in allowed_sheet_names
            elif fund_type == PRIVATE_FUND_TYPE:
                # 私募数据按照 sheet 名称筛选
                is_data_sheet = "PAGE" in sheet.Name.upper()

            # 如果不是范围内的 sheet, 则不提取数据
            if not is_data_sheet and not debug_mode:
                print(f"   -> {sheet.Name} is not in allowed_sheet_names, continue")
                for item in all_content_data:
                    if item["sheet_name"] == sheet.Name:
                        item["page"] = -1

            # 页码更新
            page_counter += 1

        all_content_data = [item for item in all_content_data if item["page"] != -1]

        # 筛选出只在正文里的单元/文本框数据
        result = []
        for content_data in all_content_data:
            sheet_name = content_data.get("sheet_name", "")
            sheet_data = dataset.get(sheet_name, {})
            article_text = sheet_data.get("split_texts", "")
            text = content_data.get("text", "")
            if not article_text:
                # 如果没有正文文本，跳过
                continue
                # result.append(content_data)
                # print(f"   -> warning: sheet {sheet_name} has no article text")
            else:
                # 如果 单元格/文本框的文本 在正文里
                _text = text.replace(" ", "").replace("\u3000", "")
                if not _text:
                    continue
                if is_text_match(_text, article_text):
                    # 比对时去掉空格，因为数据集是从 pdf 里读取的，会忽略空格; 这里比对去掉空格不会影响原数据
                    result.append(content_data)

        print(f"   -> all_content_data length: {len(all_content_data)}, result length: {len(result)}")

        return result

    @staticmethod
    def _determine_global_circle_standard(all_content):
        """
        确定序号符号后的空格标准
        Args:
            all_content:

        Returns:
        """
        all_circles = []
        circle_list = []
        for item in all_content:
            text, effective_text = item['text'], item['text']
            try:
                circle_matches = list(re.finditer(CIRCLE_REX, text))
            except Exception as e:
                traceback.print_exc()
                circle_matches = []
                print(f"   -> error - _determine_global_circle_standard: {e}")

            if not circle_matches: continue
            sort_key = (item.get('top', item.get('row', 0)), item.get('left', item.get('col', 0)))
            for match in circle_matches:
                # 保存符号
                circle = match.group(1)
                if circle:
                    circle_list.append(circle)

                # 判断空格类型
                space_after = match.group(2)
                pattern_type = "circle_no_space"
                if space_after == "\u3000":
                    pattern_type = "circle_full_space"
                elif space_after == " ":
                    pattern_type = "circle_half_space"
                all_circles.append({'page': item['page'], 'pattern': pattern_type, 'sort_key': sort_key})
        all_circles.sort(key=lambda x: (x['page'], x['sort_key']))
        patterns = {p: 0 for p in ["circle_no_space", "circle_full_space", "circle_half_space"]}
        first_occurrence = {}
        for i, circle in enumerate(all_circles):
            pattern = circle['pattern']
            patterns[pattern] += 1
            if pattern not in first_occurrence: first_occurrence[pattern] = i
        # 筛选匹配出的序号符号是否是[圆圈、双圆圈、实心圆圈、菱形、实心菱形、方块、实心方块、星形、实心星形]等类似的符号
        index_circle = ""
        if all(circle_list):
            circles = "".join(circle_list)
            res = ai_check_circle(circles, CIRCLE_CHECK_PROMPT)
            try:
                res_json = json.loads(res)
                index_circle = res_json.get('result')
                print(f"   -> index_circle: {index_circle}")
            except Exception as e:
                # print(f"   -> ai_check_circle result: {res.replace("\n", " ")}")
                print(f"   -> error: parse ai_check_circle result: {e}")

                # 如果 大模型调用失败，则用规则处理
                index_circle = "".join(list(set(circle_list)))
                print(f"   -> index_circle: {index_circle}")

        if not any(patterns.values()):
            print(f"   -> no_circle")
            return "no_circle", index_circle
        return max(patterns.keys(), key=lambda k: (patterns[k], -first_occurrence.get(k, float('inf')))), index_circle

    def check_color_space_error(self, all_content, fund_type):
        """
        检查颜色和空格
        Args:
            all_content: 正文范围内的所有单元格/文本框等 shape 的数据
            fund_type:

        Returns:
        """
        if not all_content:
            return [], []
        # 获取序号符号后的空格类型
        global_standard_pattern, index_circle = self._determine_global_circle_standard(all_content)
        error_total = []

        ordered_valid_sheets = []
        for item in all_content:
            # 按照原始顺序构建有效工作表列表，并防止重复
            sheet_name = item['sheet_name']
            if sheet_name not in ordered_valid_sheets:
                ordered_valid_sheets.append(sheet_name)

            # 检查颜色
            text, page = item['text'], item['page']
            if item['source'] == 'shape' and item.get('abnormal_color_text'):
                error_total.append(
                    {'original_text': item['abnormal_color_text'], 'page': page, 'reason_type': "異常な色"})
            elif item['source'] == 'cell' and not self.is_allowed_color(item.get('font_color', 0)):
                error_total.append({'original_text': text, 'page': page, 'reason_type': "異常な色"})

            # 检查空格
            error_list, _ = self.get_space(text, page, fund_type, global_standard_pattern, index_circle)
            error_total.extend(error_list)

        # 返回正确顺序的列表
        return error_total, ordered_valid_sheets

    @staticmethod
    def deduplicate_errors(error_total):
        """
        比对结果去重
        Args:
            error_total:

        Returns:
        """
        if not error_total: return []
        temp_results, seen = [], set()
        for item in error_total:
            key = (item['original_text'], item['page'], item['reason_type'])
            if key not in seen:
                seen.add(key)
                temp_results.append(item)
        final_error_total = []
        temp_results.sort(key=lambda x: len(x['original_text']))
        for item in temp_results:
            text1, is_redundant = item["original_text"], False
            for existing_item in final_error_total:
                if item["page"] != existing_item["page"] or item["reason_type"] != existing_item[
                    "reason_type"]: continue
                text2 = existing_item["original_text"]
                if text2 in text1:
                    is_redundant = True
                    break
            if not is_redundant:
                final_error_total.append(item)
        return final_error_total

    @staticmethod
    def read_excel(excel, excel_path: str):
        """
        读取 excel 文件
        Args:
            excel:
            excel_path:

        Returns:
            workbook
        """
        try:
            workbook = excel.Workbooks.Open(
                os.path.abspath(excel_path), UpdateLinks=0, ReadOnly=True,
                IgnoreReadOnlyRecommended=True, CorruptLoad=1, Password=""
            )
            print("   -> excel file open success")
            return workbook
        except Exception as second_error:
            print(f"   -> error - read_excel: {second_error}")
            workbook = None
            return workbook


    def convert_excel(self, excel_path):
        """
        分析Excel文件，转换为PDF。
        在第一次打开失败后，会先彻底关闭Excel实例，再调用VBA修复，然后用新实例重试。
        """
        pythoncom.CoInitialize()
        excel = None
        workbook = None
        final_errors = []

        try:
            # 第一种方法
            print(f"   -> [method 1/2] opening excel file: {os.path.basename(excel_path)}")
            excel = win32com.client.DispatchEx("Excel.Application")
            excel.Visible = False
            excel.DisplayAlerts = False

            try:
                workbook = excel.Workbooks.Open(
                    os.path.abspath(excel_path), UpdateLinks=0, ReadOnly=True,
                    IgnoreReadOnlyRecommended=True, CorruptLoad=1, Password=""
                )
                print("   -> open file success")

            except pythoncom.com_error as open_error:
                is_corrupt_error = (open_error.excepinfo and len(open_error.excepinfo) >= 6 and
                                    open_error.excepinfo[1] == 'Microsoft Excel' and
                                    open_error.excepinfo[5] == -2146827284)

                if is_corrupt_error:
                    print("   -> 初次打开失败，确认文件为损坏。")
                    # ==============================================================================
                    # 关闭失败的实例，释放文件锁
                    # ==============================================================================
                    print("   -> 正在关闭的 Excel 实例以及文件锁...")
                    try:
                        excel.Quit()
                    except Exception:
                        pass
                    self.cleanup_office_processes()  # 强制清理，确保文件锁释放
                    excel = None  # 标记为已处理
                    print("   -> 文件锁以释放，用 VBA 修复。")
                    # ==============================================================================

                    # 修复
                    repair_success = self._run_vba_repair_tool(excel_path)

                    if repair_success:
                        print("   -> VBA 修复成功")
                        return excel_path
                    else:
                        print("   -> VBA 修复失败")
                        return ""

            # --- 最終檢查 ---
            if workbook is None:
                print(f"   -> 无法打开文件 '{os.path.basename(excel_path)}'，停止处理")
                # 如果 excel 對象還存在（例如在非損壞錯誤中），清理它
                if excel:
                    try:
                        excel.Quit()
                    except:
                        pass
                    self.cleanup_office_processes()
                return None
            return excel_path
        except Exception as e:
            traceback.print_exc()
            print(f"convert_to_pdf: {e}")
            return ""
        finally:
            if workbook:
                try:
                    workbook.Close(SaveChanges=False)
                except:
                    pass
            if excel:
                try:
                    excel.Quit()
                except:
                    pass
            pythoncom.CoUninitialize()


    def _run_vba_repair_tool(self, corrupted_file_path):
        """
        使用一个独立的Excel实例，调用 vba_tool.xlsm 中的宏来修复文件。
        这个函数假设它是在一个已经初始化了COM的线程中被调用的。
        """
        # vba_tool_path = resource_path('vba_tool.xlsm')
        vba_tool_path = resource_path(VBA_EXCEL_FILE)
        print(f"   -> [VBA 修复] 正在启动 VBA 修复工具: {os.path.basename(vba_tool_path)}")

        if not os.path.exists(vba_tool_path):
            print(f"   -> [VBA 修复] 错误: VBA 工具 'vba_tool.xlsm' 未在程序根目录找到。")
            return False

        vba_excel = None
        tool_workbook = None
        is_success = False

        try:
            vba_excel = win32com.client.DispatchEx("Excel.Application")
            vba_excel.Visible = False
            vba_excel.DisplayAlerts = False

            tool_workbook = vba_excel.Workbooks.Open(vba_tool_path, ReadOnly=True)

            print(
                f"   -> [VBA 修复] 正在对文件 '{os.path.basename(corrupted_file_path)}' 执行 RepairAndOverwrite 宏...")

            result = vba_excel.Application.Run("RepairAndOverwrite", os.path.abspath(corrupted_file_path))

            if result is True:
                print("   -> [VBA 修复] VBA 宏报告修复并覆盖成功。")
                is_success = True
            else:
                print("   -> [VBA 修复] VBA 宏报告修复失败。请检查 vba_tool.xlsm 的日志或代码。")
                is_success = False

        except Exception as e:
            print(f"   -> [VBA 修复] 调用 VBA 修复工具时发生严重错误: {e}")
            is_success = False
        finally:
            if tool_workbook:
                try:
                    tool_workbook.Close(SaveChanges=False)
                except Exception:
                    pass
            if vba_excel:
                try:
                    vba_excel.Quit()
                except Exception:
                    pass
            tool_workbook = None
            vba_excel = None

        return is_success


def is_file_valid_and_ready(file_path, allowed_extensions, wait_timeout=10):
    """
    检查文件是否有效：
    1. 过滤掉临时文件 (如 ~$...).
    2. 检查文件后缀名是否在允许列表中。
    3. 检查文件是否已完成复制（不再被锁定）。
    """
    file_name = os.path.basename(file_path)

    # 步骤 1: 过滤掉临时文件
    if file_name.startswith('~$') or file_name.startswith('~'):
        print(f"   -> 忽略Office临时文件: {file_name}")
        return False

    # 步骤 2: 检查文件后缀名
    file_extension = os.path.splitext(file_name)[1].lower()
    if file_extension not in allowed_extensions:
        return False

    # 步骤 3: 检查文件是否完成复制
    start_time = time.time()
    while time.time() - start_time < wait_timeout:
        try:
            # 使用 'rb' 模式进行只读检查，减少权限问题
            with open(file_path, 'rb'):
                return True
        except IOError:
            time.sleep(0.5)

    print(f"   -> 跳过文件，因为它在 {wait_timeout} 秒后仍被锁定: {file_name}")
    return False


if __name__ == "__main__":

    # circles = "・※("
    # res = ai_check_circle(circles, CIRCLE_CHECK_PROMPT)
    # if res:
    #     res_json = json.loads(res)
    #     index_circle = res_json.get('result')
    #     print(f"   -> index_circle: {index_circle}")

    pass
