import abc
import collections
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

    def ingest(self, data: typing.Any) -> None:
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


def test_numeric_processor() -> None:
    print("Testing Numeric Processor...")
    numeric_processor = NumericProcessor()

    print("Trying to validate input '42':")
    print(numeric_processor.validate(42))

    print("Trying to validate input 'Hello':")
    print(numeric_processor.validate("Hello"))

    print("Test invalid ingestion of string 'foo' without prior "
          "validation:")
    try:
        numeric_processor.ingest("foo")
    except ValueError as error:
        print("Got exception:")
        print(error)

    print("Processing data:")
    numeric_data: list[int | float] = [1, 2, 3, 4, 5]
    print(numeric_data)
    numeric_processor.ingest(numeric_data)

    print("Extracting 3 values...")
    extraction_count = 0
    while extraction_count < 3:
        rank, value = numeric_processor.output()
        print(f"Numeric value {rank}:", value)
        extraction_count = extraction_count + 1
    print()


def test_text_processor() -> None:
    print("Testing Text Processor...")
    text_processor = TextProcessor()

    print("Trying to validate input '42':")
    print(text_processor.validate(42))

    print("Processing data:")
    text_data = ["Hello", "Nexus", "World"]
    print(text_data)
    text_processor.ingest(text_data)

    print("Extracting 1 value...")
    rank, value = text_processor.output()
    print(f"Text value {rank}:")
    print(value)
    print()


def test_log_processor() -> None:
    print("Testing Log Processor...")
    log_processor = LogProcessor()

    print("Trying to validate input 'Hello':")
    print(log_processor.validate("Hello"))

    print("Processing data:")
    log_data = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    print(log_data)
    log_processor.ingest(log_data)

    print("Extracting 2 values...")
    extraction_count = 0
    while extraction_count < 2:
        rank, value = log_processor.output()
        print(f"Log entry {rank}:", value)
        extraction_count = extraction_count + 1


def main() -> None:
    print("=== Code Nexus - Data Processor ===")
    print()
    test_numeric_processor()
    test_text_processor()
    test_log_processor()


if __name__ == "__main__":
    main()
