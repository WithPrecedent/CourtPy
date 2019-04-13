""" 
LogitEncoder is a class that encodes levels of categorical variables using 
logistic regression coefficients  at each category level. This allows the user 
to convert high-cardinality features, which are problematic for many machine 
learning models into continuous features.

LogitEncoder is computationally expensive but may increase predictive 
accuracy versus weight of evidence and target encoding because it controls 
for other features in assigning an encoded value to each category level.

As with any categorical encoder that incorporates the predicted label, it is 
important that LogitEncoder be fitted with an isolated training dataset to 
prevent data leakage. 
"""
from dataclasses import dataclass
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

import category_encoders as ce

from ml_funnel.methods import Methods

@dataclass
class LogitEncoder(Methods):
    
    """ 
    Parameters:
        
    cols: list 
    A list of columns to encode.
    
    first_encoder: str
    To apply the logistic regression encoding, the data needs to first 
    be converted from categorical to a usable format. This parameter
    sets which of the encoders to use from the category_encoders package.

    threshold: int
    A value which sets the number of instances for a value to appear
    in a single column from the cols list in order to be 
    encoded. If there are the number of instances is equal to or 
    below the threshold setting, an encoded value will not be used.
        
    below_threshold: str
    If threshold is greater than 0 then below_threshold sets what 
    value should be used in place of a logistic-regression-based 
    value for that particular instance. If set to 'mean', the mean
    of the column will be used. If set to 'zero', the value of 0 
    will be used.
            
    verbose: bool 
    A value of True will include progress updates. verbose of False 
    will not include any progress updates.

    """
    cols : object = None
    first_encoder : str = 'helmert'
    threshold : int = 0
    below_threshold : str = 'mean'
    verbose : str = True   

    def __post_init__(self):
        self._set_first_encoder()
        return self
       
    def _set_first_encoder(self):
        if self.first_encoder == 'backward':
            self.first = ce.BackwardDifferenceEncoder(cols = self.cols)
        if self.first_encoder == 'binary':
            self.first = ce.BinaryEncoder(cols = self.cols)
        if self.first_encoder == 'sum':
            self.first = ce.SumEncoder(cols = self.cols)
        if self.first_encoder == 'helmert':
            self.first = ce.HelmertEncoder(cols = self.cols) 
        return self   
    
    def _find_cat_cols(self, x):
        cat_cols = []
        for col in x.columns:
            if x[col].dtype == 'category':
                cat_cols.append(col)
            elif x[col].dtype == str:
                x[col] = x[col].astype('category')
                cat_cols.append(col)
        self.cols = cat_cols
        return self
  
    def _convert_below_threshold(self, x):           
        for col in self.cols:
            if self.below_threshold == 'zero':
                self.below_threshold_value = 0
            elif self.below_threshold == 'mean':
                self.below_threshold_value = np.mean(x[col])
            x['value_counts'] = x[col].value_counts()
            x[col] = np.where(x['value_counts'] <= self.threshold,
                              self.below_threshold_value, x[col]) 
        x.drop('value_counts', axis = 'columns', inplace = True)       
        return x
   
    def fit(self, x, y, **kwargs):
        if not self.cols:
            self._find_cat_cols(x)
        if self.threshold > 0:
            x = self._convert_below_threshold(x)
        self.first.fit_transform(x, y)
        self.fit_logit = LogisticRegression(**kwargs).fit(x, y)
        return x
    
    def transform(self, x, y = None, **kwargs):
        for col in self.cols:
            coefs = pd.DataFrame(list(zip(x.columns, self.fit_logit.coef_)))
            self.encoder_dict = coefs.to_dict()
            self.x[col] = self.coef_dict.get(self.x[col])
        return x
        
    def fit_transform(self, x, y, **kwargs):        
        x = self.fit(x, y, **kwargs)
        x = self.transform(x, y, **kwargs)
        return x
