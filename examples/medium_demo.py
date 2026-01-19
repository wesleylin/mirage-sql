from dataclasses import dataclass
from mirage_sql import mirror

@dataclass
class Student:
    student_id: int
    name: str
    hp: int = 0


@dataclass 
class Room:
    room_id: int
    name: str
    size: int


@dataclass
class StudentAssignment:
    student_id: int 
    room_id: int 


def main():
    students = mirror([Student(23, "Kyle"), (Student(74, "Stanley"))])
    rooms = mirror([Room(7, "Science", 10), Room(90, "Chemistry", 9)])

    # studentAssignment = mirror

main()