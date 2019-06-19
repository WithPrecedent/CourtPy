"""
Parent class for CourtAnalyzer containing methods for analysis stage and
all CourtTool methods as well.
"""

from dataclasses import dataclass

from library.case_tool import CaseTool

@dataclass
class CaseAnalyzer(CaseTool):

    def __post_init__(self):
        super().__post_init__()
        return

    def add_splices(self, splice_dict = None):
        if not splice_dict:
            splice_dict = self.settings['splicers_params']
        if self.settings['splicers_params']['prefixes']:
            for name, splice in splice_dict.items():
                prefixes = self.funnel._listify(splice)
                if not name in ['include_all', 'prefixes']:
                    self.funnel.add_splice(splice = name, prefixes = prefixes)
        else:
            for name, splice in splice_dict.items():
                cols = self.funnel._listify(splice)
                if not name in ['include_all', 'prefixes']:
                    self.funnel.add_splice(splice = name, cols = cols)
        return self