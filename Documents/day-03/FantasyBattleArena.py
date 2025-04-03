class Character:
    def __init__(self,n,h,ap,d,s):
        self.name=n
        self.health=h
        self.attack_power=ap
        self.defense=d
        self.speed=s

    def attack(self,target):
        target.take_damage(self.attack_power)

    def take_damage(self,amount):
        self.health-=amount
        return True

    def is_alive(self):
        if self.health<=0:
            return False
        else:
            return True


class Warrior(Character):
    def __init__(self,r,n,h,ap,d,s):
        super().__init__(n,h,ap,d,s)
        self.rage=r
    def rage(self):
        if self.rage>=10:
            self.attack_power+=10
    def spl(self):
        if self.health<30:
            self.attack_power *= 2

class Mage(Character):
    def __init__(self,m,n,h,ap,d,s):
        super().__init__(n,h,ap,d,s)
        self.mana=m
    def spcl(self):
        #

class Archer(Character):
    def __init__(self,cc,n,h,ap,d,s):
        super().__init__(n,h,ap,d,s)
        self.critical_chance=cc
    def spl_ability(self):
        #


       