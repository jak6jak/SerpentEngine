import pygame


class Game:
    """
    the main controller for the entire game
    """
    def __init__(self, width=640, height=480):
        pygame.init()
        self.screen = pygame.display.set_mode([width, height])
