"""
Startup Sequences Module

Organizes bot startup into separate phases for better maintainability.
Each sequence is a self-contained module that handles one phase of startup.
"""

__all__ = [
    'sequence_1_initialization',
    'sequence_2_tracking',
    'sequence_3_server_status',
    'sequence_4_file_sync',
    'sequence_5_channels',
    'sequence_6_background'
]

