from collections import OrderedDict

import pandas as pd

from .utils import sanitize_df


def parse_data(backend, rows, headers=True, columns=None):
    df_dict = OrderedDict()
    col_index = {}
    for i, row in enumerate(rows):
        # row is a list of cells
        if headers and i == 0 and not columns:
            repeat_until = -1
            repeat_value = None
            # columns as lists in a dictionary
            columns = []
            # parse the first row as column names
            for k, cell in enumerate(row):

                if cell.tag == "{"+cell.nsmap["table"]+"}" + "covered-table-cell":
                    continue
                elif cell.tag == "{"+cell.nsmap["table"]+"}" + "table-cell":
                    value, n_repeated, n_spanned = backend.get_value(cell)

                # CB REM 20220309 purpose of the repeated impl. is unclear
                # for our puposes it is getting in the way -> disabling it
                #if n_repeated > 0:
                #    repeat_value = value
                #    repeat_until = n_repeated + k
                #if not value and k <= repeat_until:
                #    value = repeat_value
                #if k == repeat_until:
                #    # reset to allow for more than one repeated column
                #    repeat_until = -1

                # CB EXT 20220309
                # it is important to address spanning cols properly
                # otherwise the indecies of the final dataset will be messed up
                # 
                # based on our fods example (that uses spanning rows and cols)
                # it is assumed that fods (xml) data contains the maximum number of
                # columns and rows that contain data. However, due to the spanning
                # *especially* columns are optimized via table:convered-table-cell 
                # nodes with table:number-columns-repeatet attributes.
                # such a node may be empty while still providing repeated information.
                # To get a proper dataset representation we need to add these
                # repeated cells!
                if n_repeated == 0  and n_spanned == 0:
                    n_repeat = 1
                elif n_repeated > 0 and n_spanned == 0:
                    n_repeat = n_repeated
                elif n_repeated == 0 and n_spanned > 0:
                    if n_spanned-1 <= 0:
                        n_repeat = 1 # need at least one pass for a normal non-spanned cell
                    else:
                        n_repeat = n_spanned
                #elif n_repeated  > 0 and n_spanned == 0:
                #    n_repeat = n_repeated
                for xx in range(n_repeat):                
                    if value and value not in columns:
                        columns.append(value)
                    else:
                        column_name = value if value else "unnamed"
                        # add count to column name
                        idx = 1
                        while f"{column_name}.{idx}" in columns:
                            idx += 1
                        columns.append(f"{column_name}.{idx}")
        elif i == 0:
            # without headers, assign generic numbered column names
            columns = columns if columns else [f"column.{j}" for j in range(len(row))]
        if i == 0:
            df_dict = OrderedDict((column, []) for column in columns)
            # create index for the column headers
            col_index = {j: column for j, column in enumerate(columns)}
            if headers:
                continue
        # CB 20220309 - this implementation is causing issue if there are differnt joined (covered-table-cells)
        # and if the content header is using multiple lines. 
        k = 0
        for j, cell in enumerate(row):
            if j < len(col_index) and k < len(col_index):
                value, n_repeated, n_spanned = backend.get_value(cell, parsed=True)

                # repeated attributes shall only result into auto cell duplication if it is
                # part of a covered-table-cell which *must* follow a spanned element
                if (n_repeated == 0 and n_spanned == 0 ) and cell.tag == "{"+cell.nsmap["table"]+"}" + "table-cell":
                    n_repeat = 1
                elif (n_repeated > 0 and n_spanned == 0 ) and cell.tag == "{"+cell.nsmap["table"]+"}" + "table-cell":
                    n_repeat = n_repeated
                elif (n_repeated == 0 and n_spanned > 0 ) and cell.tag == "{"+cell.nsmap["table"]+"}" + "table-cell":
                    if n_spanned -1 <= 0:
                        n_repeat = 1
                    else:
                        n_repeat = n_spanned                    
                elif cell.tag == "{"+cell.nsmap["table"]+"}" + "covered-table-cell":
                    continue
                    
                for xx in range(n_repeat):
                    # use header instead of column index
                    df_dict[col_index[k]].append(value)
                    k += 1
            else:
                raise ValueError("j index went out of bound")

    # make sure all columns are of the same length
    max_col_length = max(len(df_dict[col]) for col in df_dict)
    for col in df_dict:
        col_length = len(df_dict[col])
        if col_length < max_col_length:
            df_dict[col] += [None] * (max_col_length - col_length)
    df = pd.DataFrame(df_dict)
    return df


def read_data(backend, file_or_path, sheet_id, headers=True, columns=None):
    doc = backend.get_doc(file_or_path)
    rows = backend.get_rows(doc, sheet_id)
    df = parse_data(backend, rows, headers=headers, columns=columns)
    return df
    #return sanitize_df(df)
