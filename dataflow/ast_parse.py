# -*- coding: utf-8 -*-

import argparse
import ast
import contextlib
import json
import sys
from ast import Name, Load
from typing import Any
from typing import Generator
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type
from typing import TYPE_CHECKING
from typing import Union

if TYPE_CHECKING:
    from typed_ast import ast27
    from typed_ast import ast3

    ASTType = Union[ast.AST, ast27.AST, ast3.AST]

AST: Tuple[Type[Any], ...] = (ast.AST,)
expr_context: Tuple[Type[Any], ...] = (ast.expr_context,)
try:  # pragma: no cover (with typed-ast)
    from typed_ast import ast27
    from typed_ast import ast3
except ImportError:  # pragma: no cover (without typed-ast)
    typed_support = False
else:  # pragma: no cover (with typed-ast)
    AST += (ast27.AST, ast3.AST)
    expr_context += (ast27.expr_context, ast3.expr_context)
    typed_support = True


def _is_sub_node(node: Any) -> bool:
    return isinstance(node, AST) and not isinstance(node, expr_context)


def _is_leaf(node: 'ASTType') -> bool:
    for field in node._fields:
        attr = getattr(node, field)
        if _is_sub_node(attr):
            return False
        elif isinstance(attr, (list, tuple)):
            for val in attr:
                if _is_sub_node(val):
                    return False
    else:
        return True


def _fields(n: 'ASTType', show_offsets: bool = True) -> Tuple[str, ...]:
    if show_offsets:
        return n._attributes + n._fields
    else:
        return n._fields


def _leaf(node: 'ASTType', show_offsets: bool = True, verbose=False):
    if isinstance(node, AST):
        if isinstance(node, Name):
            if verbose:
                print('_leaf if node', node.__dict__)
            res = []
            for field in _fields(node, show_offsets=show_offsets):
                tmp = _leaf(getattr(node, field), show_offsets=show_offsets)
                if tmp:
                    res.append(tmp)
            return res
        else:
            # 只考虑 Name 跟 Load 情况下的expr
            # assert isinstance(node, Load)
            return []
    elif isinstance(node, list):
        if verbose:
            print('_leaf elif node', node)
        return [_leaf(x, show_offsets=show_offsets) for x in node]
    else:
        if verbose:
            print('_leaf else node', node)
        return str(node)


def pformat(
        node: Union['ASTType', None, str],
        indent: Union[str, int] = '    ',
        show_offsets: bool = True,
        _indent: int = 0,
) -> str:
    if node is None:
        return repr(node)
    elif isinstance(node, str):  # pragma: no cover (ast27 typed-ast args)
        return repr(node)
    elif _is_leaf(node):
        return _leaf(node, show_offsets=show_offsets)
    else:
        if isinstance(indent, int):
            indent_s = indent * ' '
        else:
            indent_s = indent

        class state:
            indent = _indent

        @contextlib.contextmanager
        def indented() -> Generator[None, None, None]:
            state.indent += 1
            yield
            state.indent -= 1

        def indentstr() -> str:
            return state.indent * indent_s

        def _pformat(el: Union['ASTType', None, str], _indent: int = 0) -> str:
            return pformat(
                el, indent=indent, show_offsets=show_offsets,
                _indent=_indent,
            )

        out = type(node).__name__ + '(\n'
        with indented():
            for field in _fields(node, show_offsets=show_offsets):
                attr = getattr(node, field)
                if attr == []:
                    representation = '[]'
                elif (
                        isinstance(attr, list) and
                        len(attr) == 1 and
                        isinstance(attr[0], AST) and
                        _is_leaf(attr[0])
                ):
                    representation = f'[{_pformat(attr[0])}]'
                elif isinstance(attr, list):
                    representation = '[\n'
                    with indented():
                        for el in attr:
                            representation += '{}{},\n'.format(
                                indentstr(), _pformat(el, state.indent),
                            )
                    representation += indentstr() + ']'
                elif isinstance(attr, AST):
                    representation = _pformat(attr, state.indent)
                else:
                    representation = repr(attr)
                out += f'{indentstr()}{field}={representation},\n'
        out += indentstr() + ')'
        return out


def pprint(*args: Any, **kwargs: Any) -> None:
    print(pformat(*args, **kwargs))


def tree2json(
        node: Union['ASTType', None],
        show_offsets: bool = False,
        _indent: int = 0,
) -> list:
    if node is None:
        return []
    elif _is_leaf(node):
        return _leaf(node, show_offsets=show_offsets)
    else:
        class state:
            indent = _indent

        @contextlib.contextmanager
        def indented() -> Generator[None, None, None]:
            state.indent += 1
            yield
            state.indent -= 1

        def _2json(el: Union['ASTType', None, str], _indent: int = 0) -> list:
            return tree2json(
                el, show_offsets=show_offsets, _indent=_indent,
            )

        res = []
        with indented():
            tmp = {}
            for field in _fields(node, show_offsets=show_offsets):
                if field == 'keywords':
                    continue
                attr = getattr(node, field)
                if attr == []:
                    representation = []
                elif (
                        isinstance(attr, list) and
                        len(attr) == 1 and
                        isinstance(attr[0], AST) and
                        _is_leaf(attr[0])
                ):
                    representation = _2json(attr[0])
                elif isinstance(attr, list):
                    representation = []
                    with indented():
                        for el in attr:
                            representation.append(_2json(el, state.indent))
                elif isinstance(attr, AST):
                    representation = _2json(attr, 0)
                else:
                    representation = [attr]
                tmp[field] = representation
            res.append(tmp)
        return res


def get_all_name(string):
    ast_obj = ast.parse(string, mode="exec")
    body = ast_obj.body[0].__dict__.get("value")
    func_name = []
    args_name = []
    js = tree2json(body)

    def _recursion(x: list):
        if isinstance(x, str):
            args_name.append(x)
        elif isinstance(x, dict):
            func_name.extend(x.get('func'))
            return _recursion(x.get('args'))
        else:
            for js in x:
                _recursion(js)

    _recursion(js)

    return list(set(func_name)), list(set(args_name))


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument(
        '--no-show-offsets', dest='show_offsets',
        action='store_false',
    )
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument(
        '--untyped', default=ast.parse,
        dest='parse_func', action='store_const', const=ast.parse,
        help='(default) Use the stdlib `ast` parser.',
    )
    if typed_support:  # pragma: no cover (requires typed-ast)
        grp.add_argument(
            '--typed-27',
            dest='parse_func', action='store_const', const=ast27.parse,
            help='Use typed_ast.ast27 to parse the ast.',
        )
        grp.add_argument(
            '--typed-3',
            dest='parse_func', action='store_const', const=ast3.parse,
            help='Use typed_ast.ast3 to parse the ast.',
        )
    args = parser.parse_args(argv)

    type_comments = args.parse_func is ast.parse and sys.version_info >= (3, 8)
    if type_comments:  # pragma: no cover (py38+)
        kwargs = {'type_comments': True}
    else:  # pragma: no cover (<py38)
        kwargs = {}

    with open(args.filename, 'rb') as f:
        contents = f.read()
    pprint(args.parse_func(contents, **kwargs), show_offsets=args.show_offsets)
    return 0


if __name__ == '__main__':
    s = "join(i_algo_kaihei_room_name_tag_list,',')"
    s0 = "str(get_dflt(js2dict(get_dflt(js2dict(i_room_kaihei_roompara_info),'主题',{})),'扩列聊天',''))"
    s1 = 'int2float(get(a,b,c))'
    s2 = 'If(ge(len(intersection(u_user_music_ita_stay_users_cnt_28d,user_id_list)),batch(INT_1)),batch(INT_1),batch(INT_0))'

    # ast_obj = ast.parse(s2, mode="exec")
    # body = ast_obj.body[0].__dict__.get("value")
    # res = tree2json(body)
    # print(json.dumps(res, indent=2))
    # print('*' * 50)
    # from ast_parse import pformat
    # print(pformat(body))
    print('*' * 50)
    print(get_all_name(s0))
