from enum import Enum

from app.dataclass.enums.board import archBoard, cseBoard, dormBoard, mseBoard, ideBoard, iteBoard, mechanicalBoard, \
    mechatronicsBoard, schoolBoard, simBoard, aceBoard

archCode = [["b30401000000", "0122"], ["b30402000000", "0124"], ["b30403000000", "0125"]]
cseCode = [["b10601000000", "0105"], ["b10602000000", "0106"], ["b10704000000", "0109"], ["b10603000000", "0107"]]
dormCode = [["0502", "notice_board"]]
mseCode = [["b40501000000", "0130"], ["b40502000000", "0131"], ["b40503000000", "0133"]]
aceCode = [["b50501000000", "0137"], ["b50502000000", "0138"], ["b50503000000", "0139"], ["b50504000000", "0140"]]
ideCode = [["b20601000000", "0114"], ["b20602000000", "0115"]]
iteCode = [["a90601000000", "0097"], ["a90603000000", "0100"]]
mechanicalCode = [["a70601000000", "0072"], ["a70602000000", "0073"], ["a70603000000", "0074"],
                  ["a70604000000", "0075"]]
mechatronicsCode = [["a80701000000", "0085"], ["a80702000000", "0086"], ["a80704000000", "0088"],
                    ["a80705000000", "0089"]]
schoolCode = [["a10604010000", "14"], ["a10604020000", "15"], ["a10604030000", "16"]]
simCode = [["b60501000000", "0145"]]


class Department(Enum):
    ARCH = ("ARCH", archBoard, archCode)
    CSE = ("CSE", cseBoard, cseCode)
    DORM = ("DORM", dormBoard, dormCode)
    MSE = ("MSE", mseBoard, mseCode)
    ACE = ("ACE", aceBoard, aceCode)
    IDE = ("IDE", ideBoard, ideCode)
    ITE = ("ITE", iteBoard, iteCode)
    MECHANICAL = ("MECHANICAL", mechanicalBoard, mechanicalCode)
    MECHATRONICS = ("MECHATRONICS", mechatronicsBoard, mechatronicsCode)
    SCHOOL = ("SCHOOL", schoolBoard, schoolCode)
    SIM = ("SIM", simBoard, simCode)

    def __init__(self, department, boards, code):
        self.department = department
        self.boards = boards
        self.code = code

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value

    def board_len(self):
        return len(self.boards)
