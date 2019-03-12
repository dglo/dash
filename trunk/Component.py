#!/usr/bin/env python


class Component(object):
    def __init__(self, name, id, logLevel=None, host=None):
        self.__name = name
        self.__id = id
        self.__logLevel = logLevel
        self.__host = host

    def __lt__(self, other):
        if self.__name < other.__name:
            return True
        if self.__id < other.__id:
            return True
        return False

    def __str__(self):
        return self.fullname

    def __repr__(self):
        return self.fullname

    @property
    def fullname(self):
        if self.__id == 0 and not self.isHub:
            return self.__name
        return "%s#%d" % (self.__name, self.__id)

    @property
    def host(self):
        return self.__host

    @host.setter
    def host(self, newhost):
        self.__host = newhost

    @property
    def id(self):
        return self.__id

    @property
    def isBuilder(self):
        "Is this an eventBuilder or secondaryBuilders component?"
        return self.__name.lower().find("builder") >= 0

    @property
    def isHub(self):
        "Is this a stringHub component?"
        return self.__name.lower().find("hub") >= 0

    @property
    def isLocalhost(self):
        return self.__host is not None and \
            (self.__host == "localhost" or self.__host == "127.0.0.1")

    @property
    def isRealHub(self):
        "Is this a stringHub component running at the South Pole?"
        return self.__name.lower() == "stringhub" and self.__id < 1000

    @property
    def isTrigger(self):
        "Is this a trigger component?"
        return self.__name.lower().find("trigger") >= 0

    @property
    def logLevel(self):
        return self.__logLevel

    @property
    def name(self):
        return self.__name

    @property
    def num(self):
        return self.__id

    def setLogLevel(self, lvl):
        self.__logLevel = lvl