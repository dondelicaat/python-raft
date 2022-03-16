import threading
from dataclasses import dataclass
from time import sleep

@dataclass
class TrafficLight:
    red: bool = False
    yellow: bool = False
    green: bool = False


class Y1:
    def __init__(self):
        self.t1 = TrafficLight(yellow=True)
        self.t2 = TrafficLight(red=True)

    # def transition(self, clock):
    #     return G2() if clock > 5 else self


class G1:
    def __init__(self):
        self.t1 = TrafficLight(green=True)
        self.t2 = TrafficLight(red=True)

    # def transition(self, clock):
    #     return Y1() if clock > 30 else self


class Y2:
    def __init__(self):
        self.t1 = TrafficLight(red=True)
        self.t2 = TrafficLight(yellow=True)
    #
    # def transition(self, clock):
    #     return G1() if clock > 5 else self
    #

class G2:
    def __init__(self):
        self.t1 = TrafficLight(red=True)
        self.t2 = TrafficLight(green=True)
    #
    # def transition(self, clock, button_pressed):
    #     if button_pressed and clock > 30:
    #         return Y2()
    #     return Y2() if clock > 60 else self


class StateMachine:
    def __init__(self, pc):
        self.pc = pc
        self.clock = 0
        self.button_pressed = False

    def set_button_pressed(self):
        self.button_pressed = True

    def transition(self):
        self.pc = self.pc.transition()

    def reset(self):
        self.clock = 0
        self.button_pressed = False

    def handle_g1(self):
        if self.clock > 30:
            self.pc = Y1()
            self.reset()

    def handle_y1(self):
        if self.clock > 5:
            self.pc = G2()
            self.reset()

    def handle_g2(self):
        if self.button_pressed and self.clock > 30:
            self.pc = Y2()
            self.reset()
        elif self.clock > 60:
            self.pc = Y2()
            self.reset()

    def handle_y2(self):
        if self.clock > 5:
            self.pc = Y1()
            self.reset()

    def run(self):
        while True:
            if isinstance(self.pc, G1):
                print(f"state: G1")
                self.handle_g1()
            elif isinstance(self.pc, Y1):
                print(f"state: Y1")
                self.handle_y1()
            elif isinstance(self.pc, G2):
                print(f"state: G2")
                self.handle_g2()
            elif isinstance(self.pc, Y2):
                print(f"state: Y2")
                self.handle_y2()


            sleep(1)
            print(f"Button is in state: {self.button_pressed}")
            print(self.clock)
            self.clock += 1


def get_input(machine: StateMachine):
    while True:
        input("Press button by hitting enter!")
        machine.set_button_pressed()


if __name__ == "__main__":
    initial_state = Y1()
    state_machine = StateMachine(pc=initial_state)
    run_thread = threading.Thread(target=state_machine.run, args=())
    button_thread = threading.Thread(target=get_input, args=(state_machine,))
    run_thread.start()
    button_thread.start()

