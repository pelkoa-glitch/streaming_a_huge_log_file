from abc import ABC, abstractmethod
import io


class Processor(ABC):
    @abstractmethod
    def compress_and_encrypt(self, data: bytes) -> bytes:
        pass

    @abstractmethod
    def decrypt_and_uncompress(self, data: bytes) -> bytes:
        pass


class Folder(ABC):
    @abstractmethod
    async def write_file(self, name: str, data: bytes):
        pass

    @abstractmethod
    async def read_file(self, name: str) -> bytes:
        """read the file"""
        pass

    @abstractmethod
    async def list_files(self) -> list[str]:
        pass


FILE_SIZE = 1024 ** 2 * 100


async def save_backup(processor: Processor, folder: Folder, stream: io.BufferedReader):
    index = 0

    while data := stream.read(FILE_SIZE):
        encrypted_data = processor.compress_and_encrypt(data)
        await folder.write_file(name=str(index), data=encrypted_data)
        index += 1 


async def restore_backup(processor: Processor, folder: Folder, stream: io.BufferedWriter):
    all_files = await folder.list_files()
    sorted_files = sorted(all_files, key=int)

    for file_name in sorted_files:
        encrypted_bytes = await folder.read_file(file_name)
        decrypted_bytes = processor.decrypt_and_uncompress(encrypted_bytes)

        stream.write(decrypted_bytes)
