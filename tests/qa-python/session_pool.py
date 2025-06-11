#
# Copyright (C) 2025 Isima, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import logging as logger

from bios import isql, login, ServiceError, ErrorCode, time, enable_threads

# Note:
# This is adapted from a user's actual application code, so left as is as much as possible
# to find any potential problems.
# There are improvement points to include this into Python SDK


class Session:
    ID = 1

    def __init__(self, session_pool):
        self.session_pool = session_pool
        self.session = None
        self.next = None
        self.prev = None
        self.id = self.ID
        print(f"item #{self.id}")
        Session.ID += 1

    def activate_session(self):
        if self.session:
            self.session.close()
        try:
            self.session = login(
                self.session_pool.endpoint, self.session_pool.username, self.session_pool.password
            )
        except Exception as e:
            logger.critical(
                "error while creating bios session: %s, user=%s, password=%s",
                e,
                self.session_pool.username,
                self.session_pool.password,
            )
        self.next = None
        self.prev = None


class SessionPool:
    _minimum_pool_size = 4

    def __init__(self, endpoint, username, password):
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.head = Session(self)
        self.tail = Session(self)
        self.head.next = self.tail
        self.tail.prev = self.head
        print(f"Initializing SessionPool")
        # enable_threads()
        self.count = self._minimum_pool_size
        for _ in range(self.count):
            logger.info(f"inside the loop")
            current = Session(self)
            current.activate_session()
            logger.info(f"session : {current.session}")
            current.prev = self.head
            current.next = self.head.next
            current.next.prev = current
            self.head.next = current
            print(f"Precreated session: {current.session}")

    def __del__(self):
        self.close()

    def get_session(self):
        item = self.tail.prev
        logger.info("length: {}".format(self.count))
        if item is self.head:
            # The pool is empty, create a new one
            item = Session(self)
            item.activate_session()
            return item

        self.tail.prev = item.prev
        self.tail.prev.next = self.tail
        self.count -= 1
        return item

    def return_session(self, item):
        item.prev = self.head
        item.next = self.head.next
        item.next.prev = item
        self.head.next = item
        self.count += 1

    def get_session_pool_list(self):
        tmp = self.head
        lst = []
        length = 0
        result = dict()
        print("inside get session pool details before loop")
        while tmp is not None:
            obj = {}
            obj["current"] = str(tmp)
            obj["prev"] = str(tmp.prev)
            obj["next"] = str(tmp.next)
            lst.append(obj)
            tmp = tmp.next
            length += 1
        result["linkedList"] = lst
        result["head detail"] = {
            "head next": str(self.head.next),
            "head prev": str(self.head.prev),
            "head": str(self.head),
        }
        result["tail detail"] = {
            "tail next": str(self.tail.next),
            "tail prev": str(self.tail.prev),
            "tail": str(self.tail),
        }
        result["size"] = length
        return result

    def close(self):
        current = self.head.next
        while current is not self.tail:
            if current.session:
                current.session.close()
            current = current.next
