from abc import ABC, abstractmethod
import io
import asyncio
import concurrent.futures



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
        pass

    @abstractmethod
    async def list_files(self) -> list[str]:
        pass


FILE_SIZE = 1024 ** 2 * 100
MAX_TASKS_LENGTH = 10


async def process_write(
        processor: Processor,
        folder: Folder, 
        data: bytes, 
        index: int, 
        loop: asyncio.AbstractEventLoop, 
        executor: concurrent.futures.ProcessPoolExecutor
    ):
    encrypted_data = await loop.run_in_executor(executor, processor.compress_and_encrypt, data)
    await folder.write_file(name=str(index), data=encrypted_data)


async def process_read(
        processor: Processor, 
        folder: Folder, 
        file_name: str, 
        loop: asyncio.AbstractEventLoop, 
        executor: concurrent.futures.ProcessPoolExecutor
    ):
    encrypted_bytes = await folder.read_file(file_name)
    decrypted_bytes = await loop.run_in_executor(executor, processor.decrypt_and_uncompress, encrypted_bytes)

    return decrypted_bytes


async def save_backup(processor: Processor, folder: Folder, stream: io.BufferedReader):
    index = 0
    tasks: set = set()
    loop = asyncio.get_running_loop()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        while data := stream.read(FILE_SIZE):
            tasks.add(process_write(
                processor=processor, 
                folder=folder, 
                data=data, 
                index=index, 
                loop=loop, 
                executor=executor
                )
            )

            if len(tasks) >= MAX_TASKS_LENGTH:
                _, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            index += 1 

    if tasks:
        await asyncio.wait(tasks)


async def restore_backup(processor: Processor, folder: Folder, stream: io.BufferedWriter):
    all_files = await folder.list_files()
    sorted_files = sorted(all_files, key=int)
    loop = asyncio.get_running_loop()
    tasks = []

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for file_name in sorted_files:
            tasks.append(process_read(
                processor=processor, 
                folder=folder, 
                file_name=file_name, 
                loop=loop, 
                executor=executor
                )
            )    

            if len(tasks) >= MAX_TASKS_LENGTH:
                task = tasks.pop(0)
                enctypted_bytes = await task
                stream.write(enctypted_bytes)

    for task in tasks:
        stream.write(await task)
