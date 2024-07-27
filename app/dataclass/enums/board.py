from enum import Enum


class Board(Enum):
    NOTICE = "NOTICE"
    FREE = "FREE"
    JOB = "JOB"
    PDS = "PDS"
    LECTURE = "LECTURE"
    BACHELOR = "BACHELOR"
    SCHOLAR = "SCHOLAR"

    def __init__(self, board):
        self.board = board

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


archBoard = [Board.NOTICE, Board.JOB, Board.FREE]
cseBoard = [Board.NOTICE, Board.LECTURE, Board.JOB, Board.PDS]
dormBoard = [Board.NOTICE]
mseBoard = [Board.NOTICE, Board.BACHELOR, Board.PDS]
aceBoard = [Board.NOTICE, Board.BACHELOR, Board.JOB, Board.PDS]
ideBoard = [Board.NOTICE, Board.JOB]
iteBoard = [Board.NOTICE, Board.JOB]
mechanicalBoard = [Board.NOTICE, Board.LECTURE, Board.BACHELOR, Board.JOB]
mechatronicsBoard = [Board.NOTICE, Board.BACHELOR, Board.JOB, Board.FREE]
schoolBoard = [Board.NOTICE, Board.SCHOLAR, Board.BACHELOR]
simBoard = [Board.NOTICE]
