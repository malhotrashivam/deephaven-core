#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

""" This module supports reading an external Parquet files into Deephaven tables and writing Deephaven tables out as
Parquet files. """
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Union, Dict

import jpy

from deephaven import DHError
from deephaven.column import Column
from deephaven.dtypes import DType
from deephaven.table import Table, PartitionedTable
from deephaven.experimental import s3

_JParquetTools = jpy.get_type("io.deephaven.parquet.table.ParquetTools")
_JFile = jpy.get_type("java.io.File")
_JCompressionCodecName = jpy.get_type("org.apache.parquet.hadoop.metadata.CompressionCodecName")
_JParquetInstructions = jpy.get_type("io.deephaven.parquet.table.ParquetInstructions")
_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")


@dataclass
class ColumnInstruction:
    """  This class specifies the instructions for reading/writing a Parquet column. """
    column_name: Optional[str] = None
    parquet_column_name: Optional[str] = None
    codec_name: Optional[str] = None
    codec_args: Optional[str] = None
    use_dictionary: bool = False


def _build_parquet_instructions(
    col_instructions: Optional[List[ColumnInstruction]] = None,
    compression_codec_name: Optional[str] = None,
    max_dictionary_keys: Optional[int] = None,
    max_dictionary_size: Optional[int] = None,
    is_legacy_parquet: bool = False,
    target_page_size: Optional[int] = None,
    is_refreshing: bool = False,
    for_read: bool = True,
    force_build: bool = False,
    generate_metadata_files: Optional[bool] = None,
    base_name: Optional[str] = None,
    special_instructions: Optional[s3.S3Instructions] = None,
):
    if not any(
        [
            force_build,
            col_instructions,
            compression_codec_name,
            max_dictionary_keys is not None,
            max_dictionary_size is not None,
            is_legacy_parquet,
            target_page_size is not None,
            is_refreshing,
            generate_metadata_files is not None,
            base_name is not None,
            special_instructions is not None
        ]
    ):
        return _JParquetInstructions.EMPTY

    builder = _JParquetInstructions.builder()
    if col_instructions is not None:
        for ci in col_instructions:
            if for_read and not ci.parquet_column_name:
                raise ValueError("must specify the parquet column name for read.")
            if not for_read and not ci.column_name:
                raise ValueError("must specify the table column name for write.")

            builder.addColumnNameMapping(ci.parquet_column_name, ci.column_name)
            if ci.column_name:
                if ci.codec_name:
                    builder.addColumnCodec(ci.column_name, ci.codec_name, ci.codec_args)
                builder.useDictionary(ci.column_name, ci.use_dictionary)

    if compression_codec_name:
        builder.setCompressionCodecName(compression_codec_name)

    if max_dictionary_keys is not None:
        builder.setMaximumDictionaryKeys(max_dictionary_keys)

    if max_dictionary_size is not None:
        builder.setMaximumDictionarySize(max_dictionary_size)

    if is_legacy_parquet:
        builder.setIsLegacyParquet(is_legacy_parquet)

    if target_page_size is not None:
        builder.setTargetPageSize(target_page_size)

    if is_refreshing:
        builder.setIsRefreshing(is_refreshing)

    if generate_metadata_files:
        builder.setGenerateMetadataFiles(generate_metadata_files)

    if base_name:
        builder.setBaseNameForPartitionedParquetData(base_name)

    if special_instructions is not None:
        builder.setSpecialInstructions(special_instructions.j_object)

    return builder.build()

def _j_table_definition(table_definition: Union[Dict[str, DType], List[Column], None]) -> Optional[jpy.JType]:
    if table_definition is None:
        return None
    elif isinstance(table_definition, Dict):
        return _JTableDefinition.of(
            [
                Column(name=name, data_type=dtype).j_column_definition
                for name, dtype in table_definition.items()
            ]
        )
    elif isinstance(table_definition, List):
        return _JTableDefinition.of(
            [col.j_column_definition for col in table_definition]
        )
    else:
        raise DHError(f"Unexpected table_definition type: {type(table_definition)}")


class ParquetFileLayout(Enum):
    """ The parquet file layout. """

    SINGLE_FILE = 1
    """ A single parquet file. """

    FLAT_PARTITIONED = 2
    """ A single directory of parquet files. """

    KV_PARTITIONED = 3
    """ A key-value directory partitioning of parquet files. """

    METADATA_PARTITIONED = 4
    """ A directory containing a _metadata parquet file and an optional _common_metadata parquet file. """


def read(
    path: str,
    col_instructions: Optional[List[ColumnInstruction]] = None,
    is_legacy_parquet: bool = False,
    is_refreshing: bool = False,
    file_layout: Optional[ParquetFileLayout] = None,
    table_definition: Union[Dict[str, DType], List[Column], None] = None,
    special_instructions: Optional[s3.S3Instructions] = None,
) -> Table:
    """ Reads in a table from a single parquet, metadata file, or directory with recognized layout.

    Args:
        path (str): the file or directory to examine
        col_instructions (Optional[List[ColumnInstruction]]): instructions for customizations while reading particular
            columns, default is None, which means no specialization for any column
        is_legacy_parquet (bool): if the parquet data is legacy
        is_refreshing (bool): if the parquet data represents a refreshing source
        file_layout (Optional[ParquetFileLayout]): the parquet file layout, by default None. When None, the layout is
            inferred.
        table_definition (Union[Dict[str, DType], List[Column], None]): the table definition, by default None. When None,
            the definition is inferred from the parquet file(s). Setting a definition guarantees the returned table will
            have that definition. This is useful for bootstrapping purposes when the initially partitioned directory is
            empty and is_refreshing=True. It is also useful for specifying a subset of the parquet definition. When set,
            file_layout must also be set.
        special_instructions (Optional[s3.S3Instructions]): Special instructions for reading parquet files, useful when
            reading files from a non-local file system, like S3. By default, None.

    Returns:
        a table

    Raises:
        DHError
    """

    try:
        read_instructions = _build_parquet_instructions(
            col_instructions=col_instructions,
            is_legacy_parquet=is_legacy_parquet,
            is_refreshing=is_refreshing,
            for_read=True,
            force_build=True,
            special_instructions=special_instructions,
        )
        j_table_definition = _j_table_definition(table_definition)
        if j_table_definition is not None:
            if not file_layout:
                raise DHError("Must provide file_layout when table_definition is set")
            if file_layout == ParquetFileLayout.SINGLE_FILE:
                j_table = _JParquetTools.readSingleFileTable(path, read_instructions, j_table_definition)
            elif file_layout == ParquetFileLayout.FLAT_PARTITIONED:
                j_table = _JParquetTools.readFlatPartitionedTable(_JFile(path), read_instructions, j_table_definition)
            elif file_layout == ParquetFileLayout.KV_PARTITIONED:
                j_table = _JParquetTools.readKeyValuePartitionedTable(_JFile(path), read_instructions, j_table_definition)
            elif file_layout == ParquetFileLayout.METADATA_PARTITIONED:
                raise DHError(f"file_layout={ParquetFileLayout.METADATA_PARTITIONED} with table_definition not currently supported")
            else:
                raise DHError(f"Invalid parquet file_layout '{file_layout}'")
        else:
            if not file_layout:
                j_table = _JParquetTools.readTable(path, read_instructions)
            elif file_layout == ParquetFileLayout.SINGLE_FILE:
                j_table = _JParquetTools.readSingleFileTable(path, read_instructions)
            elif file_layout == ParquetFileLayout.FLAT_PARTITIONED:
                j_table = _JParquetTools.readFlatPartitionedTable(_JFile(path), read_instructions)
            elif file_layout == ParquetFileLayout.KV_PARTITIONED:
                j_table = _JParquetTools.readKeyValuePartitionedTable(_JFile(path), read_instructions)
            elif file_layout == ParquetFileLayout.METADATA_PARTITIONED:
                j_table = _JParquetTools.readPartitionedTableWithMetadata(_JFile(path), read_instructions)
            else:
                raise DHError(f"Invalid parquet file_layout '{file_layout}'")
        return Table(j_table=j_table)
    except Exception as e:
        raise DHError(e, "failed to read parquet data.") from e


def _j_file_array(paths: List[str]):
    return jpy.array("java.io.File", [_JFile(el) for el in paths])


def _j_array_of_array_of_string(data_indexes: List[List[str]]):
    return jpy.array("[Ljava.lang.String;", [jpy.array("java.lang.String", index_cols) for index_cols in data_indexes])


def delete(path: str) -> None:
    """ Deletes a Parquet table on disk.

    Args:
        path (str): path to delete

    Raises:
        DHError
    """
    try:
        _JParquetTools.deleteTable(_JFile(path))
    except Exception as e:
        raise DHError(e, f"failed to delete a parquet table: {path} on disk.") from e


def write(
    table: Table,
    path: str,
    col_definitions: Optional[List[Column]] = None,
    col_instructions: Optional[List[ColumnInstruction]] = None,
    compression_codec_name: Optional[str] = None,
    max_dictionary_keys: Optional[int] = None,
    max_dictionary_size: Optional[int] = None,
    target_page_size: Optional[int] = None,
    generate_metadata_files: Optional[bool] = None,
    index_columns: Optional[List[List[str]]] = None
) -> None:
    """ Write a table to a Parquet file.

    Args:
        table (Table): the source table
        path (str): the destination file path; the file name should end in a ".parquet" extension. If the path
            includes any non-existing directories, they are created. If there is an error, any intermediate directories
            previously created are removed; note this makes this method unsafe for concurrent use
        col_definitions (Optional[List[Column]]): the column definitions to use for writing, instead of the definitions
            implied by the table. Default is None, which means use the column definitions implied by the table
        col_instructions (Optional[List[ColumnInstruction]]): instructions for customizations while writing particular
            columns, default is None, which means no specialization for any column
        compression_codec_name (Optional[str]): the compression codec to use. Allowed values include "UNCOMPRESSED",
            "SNAPPY", "GZIP", "LZO", "LZ4", "LZ4_RAW", "ZSTD", etc. If not specified, defaults to "SNAPPY".
        max_dictionary_keys (Optional[int]): the maximum number of unique keys the writer should add to a dictionary page
            before switching to non-dictionary encoding, never evaluated for non-String columns, defaults to 2^20 (1,048,576)
        max_dictionary_size (Optional[int]): the maximum number of bytes the writer should add to the dictionary before
            switching to non-dictionary encoding, never evaluated for non-String columns, defaults to 2^20 (1,048,576)
        target_page_size (Optional[int]): the target page size in bytes, if not specified, defaults to 2^20 bytes (1 MiB)
        generate_metadata_files (Optional[bool]): whether to generate parquet _metadata and _common_metadata files,
            defaults to False. Generating these files can help speed up reading of partitioned parquet data because these
            files contain metadata (including schema) about the entire dataset, which can be used to skip reading some
            files.
        index_columns (Optional[List[List[str]]]): Lists containing the column names for indexes to persist. The write
            operation will store the index info as sidecar tables. By default, data indexes to write are determined by
            those present on the source table. This argument is used to narrow the set of indexes to write, or to be
            explicit about the expected set of indexes present on all sources. Indexes that are specified but missing
            will be computed on demand.

    Raises:
        DHError
    """
    try:
        write_instructions = _build_parquet_instructions(
            col_instructions=col_instructions,
            compression_codec_name=compression_codec_name,
            max_dictionary_keys=max_dictionary_keys,
            max_dictionary_size=max_dictionary_size,
            target_page_size=target_page_size,
            for_read=False,
            generate_metadata_files=generate_metadata_files,
        )
        if col_definitions is not None:
            table_definition = _JTableDefinition.of([col.j_column_definition for col in col_definitions])
        else:
            table_definition = table._definition

        if index_columns:
            table_array = jpy.array("io.deephaven.engine.table.Table", [table.j_table])
            index_columns_array = _j_array_of_array_of_string(index_columns)
            _JParquetTools.writeParquetTables(table_array, table_definition, write_instructions,
                                              _j_file_array([path]), index_columns_array)
        else:
            _JParquetTools.writeTable(table.j_table, path, table_definition, write_instructions)
    except Exception as e:
        raise DHError(e, "failed to write to parquet data.") from e


def write_partitioned(
        table: Union[Table, PartitionedTable],
        destination_dir: str,
        col_definitions: Optional[List[Column]] = None,
        col_instructions: Optional[List[ColumnInstruction]] = None,
        compression_codec_name: Optional[str] = None,
        max_dictionary_keys: Optional[int] = None,
        max_dictionary_size: Optional[int] = None,
        target_page_size: Optional[int] = None,
        base_name: Optional[str] = None,
        generate_metadata_files: Optional[bool] = None,
        index_columns: Optional[List[List[str]]] = None
) -> None:
    """ Write table to disk in parquet format with the partitioning columns written as "key=value" format in a nested
    directory structure. For example, for a partitioned column "date", we will have a directory structure like
    "date=2021-01-01/<base_name>.parquet", "date=2021-01-02/<base_name>.parquet", etc. where "2021-01-01" and
    "2021-01-02" are the partition values and "<base_name>" is passed as an optional parameter. All the necessary
    subdirectories are created if they do not exist.

    Args:
        table (Table): the source table or partitioned table
        destination_dir (str): The path to destination root directory in which the partitioned parquet data will be stored
            in a nested directory structure format. Non-existing directories in the provided path will be created.
        col_definitions (Optional[List[Column]]): the column definitions to use for writing, instead of the definitions
            implied by the table. Default is None, which means use the column definitions implied by the table
        col_instructions (Optional[List[ColumnInstruction]]): instructions for customizations while writing particular
            columns, default is None, which means no specialization for any column
        compression_codec_name (Optional[str]): the compression codec to use. Allowed values include "UNCOMPRESSED",
            "SNAPPY", "GZIP", "LZO", "LZ4", "LZ4_RAW", "ZSTD", etc. If not specified, defaults to "SNAPPY".
        max_dictionary_keys (Optional[int]): the maximum number of unique keys the writer should add to a dictionary page
            before switching to non-dictionary encoding; never evaluated for non-String columns , defaults to 2^20 (1,048,576)
        max_dictionary_size (Optional[int]): the maximum number of bytes the writer should add to the dictionary before
            switching to non-dictionary encoding, never evaluated for non-String columns, defaults to 2^20 (1,048,576)
        target_page_size (Optional[int]): the target page size in bytes, if not specified, defaults to 2^20 bytes (1 MiB)
        base_name (Optional[str]): The base name for the individual partitioned tables, if not specified, defaults to
            `{uuid}`, so files will have names of the format `<uuid>.parquet` where `uuid` is a randomly generated UUID.
            Users can provide the following tokens in the base_name:
            - The token `{uuid}` will be replaced with a random UUID. For example, a base name of
            "table-{uuid}" will result in files named like "table-8e8ab6b2-62f2-40d1-8191-1c5b70c5f330.parquet.parquet".
            - The token `{partitions}` will be replaced with an underscore-delimited, concatenated string of
            partition values. For example, for a base name of "{partitions}-table" and partitioning columns "PC1" and
            "PC2", the file name is generated by concatenating the partition values "PC1=pc1" and "PC2=pc2"
            with an underscore followed by "-table.parquet", like "PC1=pc1_PC2=pc2-table.parquet".
            - The token `{i}` will be replaced with an automatically incremented integer for files in a directory. For
            example, a base name of "table-{i}" will result in files named like "PC=partition1/table-0.parquet",
            "PC=partition1/table-1.parquet", etc.
        generate_metadata_files (Optional[bool]): whether to generate parquet _metadata and _common_metadata files,
            defaults to False. Generating these files can help speed up reading of partitioned parquet data because these
            files contain metadata (including schema) about the entire dataset, which can be used to skip reading some
            files.
        index_columns (Optional[List[List[str]]]): Lists containing the column names for indexes to persist. The write
            operation will store the index info as sidecar tables. By default, data indexes to write are determined by
            those present on the source table. This argument is used to narrow the set of indexes to write, or to be
            explicit about the expected set of indexes present on all sources. Indexes that are specified but missing
            will be computed on demand.

    Raises:
        DHError
    """
    try:
        write_instructions = _build_parquet_instructions(
            col_instructions=col_instructions,
            compression_codec_name=compression_codec_name,
            max_dictionary_keys=max_dictionary_keys,
            max_dictionary_size=max_dictionary_size,
            target_page_size=target_page_size,
            for_read=False,
            generate_metadata_files=generate_metadata_files,
            base_name=base_name,
        )

        table_definition = None
        if col_definitions is not None:
            table_definition = _JTableDefinition.of([col.j_column_definition for col in col_definitions])

        if index_columns:
            index_columns_array = _j_array_of_array_of_string(index_columns)
            if table_definition:
                _JParquetTools.writeKeyValuePartitionedTable(table.j_object, table_definition, destination_dir,
                                                             write_instructions, index_columns_array)
            else:
                _JParquetTools.writeKeyValuePartitionedTable(table.j_object, destination_dir, write_instructions,
                                                             index_columns_array)
        else:
            if table_definition:
                _JParquetTools.writeKeyValuePartitionedTable(table.j_object, table_definition, destination_dir,
                                                             write_instructions)
            else:
                _JParquetTools.writeKeyValuePartitionedTable(table.j_object, destination_dir, write_instructions)
    except Exception as e:
        raise DHError(e, "failed to write to parquet data.") from e


def batch_write(
    tables: List[Table],
    paths: List[str],
    col_definitions: List[Column],
    col_instructions: Optional[List[ColumnInstruction]] = None,
    compression_codec_name: Optional[str] = None,
    max_dictionary_keys: Optional[int] = None,
    max_dictionary_size: Optional[int] = None,
    target_page_size: Optional[int] = None,
    generate_metadata_files: Optional[bool] = None,
    index_columns: Optional[List[List[str]]] = None
):
    """ Writes tables to disk in parquet format to a supplied set of paths.

    Note that either all the tables are written out successfully or none is.

    Args:
        tables (List[Table]): the source tables
        paths (List[str]): the destination paths. Any non-existing directories in the paths provided are
            created. If there is an error, any intermediate directories previously created are removed; note this makes
            this method unsafe for concurrent use
        col_definitions (List[Column]): the column definitions to use for writing.
        col_instructions (Optional[List[ColumnInstruction]]): instructions for customizations while writing
        compression_codec_name (Optional[str]): the compression codec to use. Allowed values include "UNCOMPRESSED",
            "SNAPPY", "GZIP", "LZO", "LZ4", "LZ4_RAW", "ZSTD", etc. If not specified, defaults to "SNAPPY".
        max_dictionary_keys (Optional[int]): the maximum number of unique keys the writer should add to a dictionary page
            before switching to non-dictionary encoding; never evaluated for non-String columns , defaults to 2^20 (1,048,576)
        max_dictionary_size (Optional[int]): the maximum number of bytes the writer should add to the dictionary before
            switching to non-dictionary encoding, never evaluated for non-String columns, defaults to 2^20 (1,048,576)
        target_page_size (Optional[int]): the target page size in bytes, if not specified, defaults to 2^20 bytes (1 MiB)
        generate_metadata_files (Optional[bool]): whether to generate parquet _metadata and _common_metadata files,
            defaults to False. Generating these files can help speed up reading of partitioned parquet data because these
            files contain metadata (including schema) about the entire dataset, which can be used to skip reading some
            files.
        index_columns (Optional[List[List[str]]]): Lists containing the column names for indexes to persist. The write
            operation will store the index info as sidecar tables. By default, data indexes to write are determined by
            those present on the source table. This argument is used to narrow the set of indexes to write, or to be
            explicit about the expected set of indexes present on all sources. Indexes that are specified but missing
            will be computed on demand.

    Raises:
        DHError
    """
    try:
        write_instructions = _build_parquet_instructions(
            col_instructions=col_instructions,
            compression_codec_name=compression_codec_name,
            max_dictionary_keys=max_dictionary_keys,
            max_dictionary_size=max_dictionary_size,
            target_page_size=target_page_size,
            for_read=False,
            generate_metadata_files=generate_metadata_files,
        )

        table_definition = _JTableDefinition.of([col.j_column_definition for col in col_definitions])

        if index_columns:
            index_columns_array = _j_array_of_array_of_string(index_columns)
            _JParquetTools.writeParquetTables([t.j_table for t in tables], table_definition, write_instructions,
                                              _j_file_array(paths), index_columns_array)
        else:
            _JParquetTools.writeTables([t.j_table for t in tables], table_definition,
                                       _j_file_array(paths))
    except Exception as e:
        raise DHError(e, "write multiple tables to parquet data failed.") from e
