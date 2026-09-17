import abc
import collections
import csv
import io
import json
import typing


class DataProcessor(abc.ABC):

    def __init__(self, name: str) -> None:
        self.name = name
        self.storage: collections.deque[tuple[int, str]] = (
            collections.deque()
        )
        self.total_ingested = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        raise NotImplementedError

    def output(self) -> tuple[int, str]:
        if len(self.storage) == 0:
            raise IndexError(
                f"{self.name} has no stored data left to output"
            )
        oldest_item = self.storage.popleft()
        return oldest_item

    def _store_value(self, text_value: str) -> None:
        rank = self.total_ingested
        self.storage.append((rank, text_value))
        self.total_ingested = self.total_ingested + 1


class NumericProcessor(DataProcessor):

    def __init__(self) -> None:
        super().__init__("Numeric Processor")

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, bool):
            return False

        if isinstance(data, int) or isinstance(data, float):
            return True

        if isinstance(data, list):
            if len(data) == 0:
                return False
            for item in data:
                item_is_bool = isinstance(item, bool)
                item_is_int = isinstance(item, int) and item_is_bool is False
                item_is_float = isinstance(item, float)
                if item_is_int is False and item_is_float is False:
                    return False
            return True

        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if self.validate(data) is False:
            raise ValueError("Improper numeric data")

        items_to_store: list[int | float]
        if isinstance(data, list):
            items_to_store = data
        else:
            items_to_store = [data]

        for item in items_to_store:
            text_value = str(item)
            self._store_value(text_value)


class TextProcessor(DataProcessor):

    def __init__(self) -> None:
        super().__init__("Text Processor")

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True

        if isinstance(data, list):
            if len(data) == 0:
                return False
            for item in data:
                if isinstance(item, str) is False:
                    return False
            return True

        return False

    def ingest(self, data: str | list[str]) -> None:
        if self.validate(data) is False:
            raise ValueError("Improper text data")

        items_to_store: list[str]
        if isinstance(data, list):
            items_to_store = data
        else:
            items_to_store = [data]

        for item in items_to_store:
            self._store_value(item)


class LogProcessor(DataProcessor):

    def __init__(self) -> None:
        super().__init__("Log Processor")

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            return self._is_valid_log_entry(data)

        if isinstance(data, list):
            if len(data) == 0:
                return False
            for item in data:
                if isinstance(item, dict) is False:
                    return False
                if self._is_valid_log_entry(item) is False:
                    return False
            return True

        return False

    def _is_valid_log_entry(
        self, entry: dict[typing.Any, typing.Any]
    ) -> bool:
        if "log_level" not in entry:
            return False
        if "log_message" not in entry:
            return False
        if isinstance(entry["log_level"], str) is False:
            return False
        if isinstance(entry["log_message"], str) is False:
            return False
        return True

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if self.validate(data) is False:
            raise ValueError("Improper log data")

        items_to_store: list[dict[str, str]]
        if isinstance(data, list):
            items_to_store = data
        else:
            items_to_store = [data]

        for entry in items_to_store:
            level = entry["log_level"]
            message = entry["log_message"]
            text_value = f"{level}: {message}"
            self._store_value(text_value)


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CsvExportPlugin:

    def process_output(self, data: list[tuple[int, str]]) -> None:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow([value for _, value in data])
        csv_line = buffer.getvalue().rstrip("\r\n")

        print("CSV Output:")
        print(csv_line)


class JsonExportPlugin:

    def process_output(self, data: list[tuple[int, str]]) -> None:
        json_dict = {f"item_{rank}": value for rank, value in data}
        json_output = json.dumps(json_dict)

        print("JSON Output:")
        print(json_output)


class DataStream:

    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            element_was_processed = False

            for processor in self.processors:
                if processor.validate(element):
                    processor.ingest(element)
                    element_was_processed = True
                    break

            if element_was_processed is False:
                print(
                    "DataStream error - Can't process element in "
                    f"stream: {element}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")

        if len(self.processors) == 0:
            print("No processor found, no data")
            return

        for processor in self.processors:
            print(f"{processor.name}:")
            total_processed = processor.total_ingested
            remaining = len(processor.storage)
            print(
                f"    total {total_processed} items processed, "
                f"remaining {remaining} on processor"
            )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self.processors:
            collected_items: list[tuple[int, str]] = []
            extracted_count = 0

            while extracted_count < nb:
                try:
                    item = processor.output()
                except IndexError:
                    break
                collected_items.append(item)
                extracted_count = extracted_count + 1

            plugin.process_output(collected_items)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")

    print("Initialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()

    print("Registering Processors")
    numeric_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()
    data_stream.register_processor(numeric_processor)
    data_stream.register_processor(text_processor)
    data_stream.register_processor(log_processor)

    print("Send first batch of data on stream:")
    first_batch: list[typing.Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected",
            },
        ],
        42,
        ["Hi", "five"],
    ]
    print(first_batch)
    data_stream.process_stream(first_batch)
    data_stream.print_processors_stats()

    print("Send 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CsvExportPlugin()
    data_stream.output_pipeline(3, csv_plugin)
    data_stream.print_processors_stats()

    print("Send another batch of data:")
    second_batch: list[typing.Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {
                "log_level": "ERROR",
                "log_message": "500 server crash",
            },
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print(second_batch)
    data_stream.process_stream(second_batch)
    data_stream.print_processors_stats()

    print("Send 5 processed data from each processor to a JSON plugin:")
    json_plugin = JsonExportPlugin()
    data_stream.output_pipeline(5, json_plugin)
    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
