###############################################
# Custom errors for the preprocessing package #
###############################################

class NotFittedError(Exception):
    """This exception is raised in a scaler object, when a transform
    or an inverse_transform method is called before the fit method"""
