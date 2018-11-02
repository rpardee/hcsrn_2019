import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import plotnine as pn
import numpy as np
import pandas as pd
from sklearn import preprocessing
from sklearn.preprocessing import MinMaxScaler

HOME_DIR = r"//bibbity/bobbity/boo/pqi_hospitalizations/ml/"

def scale_predictors(df, predictor='naive bayes'):
  df.sort_values(by=predictor, ascending=False, inplace=True)
  df2 = df.assign(num_detected=df.hospitalized.cumsum())
  df3 = df2.assign(classifier=predictor.strip())
  # df4 = df3.assign(pred=preprocessing.scale(df3[[predictor]]))
  # df4 = df3.assign(pred=mms.fit_transform(df3[[predictor]]))
  df4 = df3.assign(num_examined=np.arange(1, df3.shape[0] + 1))
  df5 = df4[['num_detected', 'classifier', 'num_examined']]
  return(df5)

mms = MinMaxScaler()

df = pd.DataFrame.from_csv(HOME_DIR + r"preds_d.csv")
print(df.describe())

# RandmForest naive_bayes LDA SVM Random  acg_ip_risk

sv = scale_predictors(df, predictor='SVC')
# ld = scale_predictors(df, predictor='LDA')
nb = scale_predictors(df, predictor='naive_bayes')
rn = scale_predictors(df, predictor='Random')
ac = scale_predictors(df, predictor='acg_ip_risk')
rf = scale_predictors(df, predictor='RandmForest')
ct = scale_predictors(df, predictor='cheating')

df2 = pd.concat([nb, rn, ac, rf, sv, ct])
# df2 = pd.concat([nb, rn, ac, rf, ct])

print(df2.head(20))
print(df2.describe())
p = pn.ggplot(df2, pn.aes(x='num_examined', y='num_detected', group='classifier', colour='classifier')) +\
    pn.geom_step() +\
    pn.ggtitle("How Many ppl would we need to intervene on to prevent Y hospitalizations?")
    # pn.scales.scale_x_reverse()

p.save(HOME_DIR + 'all_together_d.png', height=8, width=10, units='in', verbose=False)

p2 = pn.ggplot(df2, pn.aes(x='num_examined', y='num_detected', group='classifier', colour='classifier')) +\
    pn.geom_step() +\
    pn.ggtitle("How Many ppl would we need to intervene on to prevent Y hospitalizations?") +\
    pn.xlim(0, 300) + pn.ylim(0, 300)
    # pn.scales.scale_x_reverse()

p2.save(HOME_DIR + 'all_together_trunc.png', height=8, width=10, units='in', verbose=False)


print("Finished!")
