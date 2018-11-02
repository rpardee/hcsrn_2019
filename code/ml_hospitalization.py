import pyodbc
import numpy as np
import pandas as pd
from collections import defaultdict
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.externals import joblib
from sklearn.naive_bayes import GaussianNB
from sklearn.linear_model import Perceptron
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, accuracy_score
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.svm import LinearSVC
from sklearn.ensemble import RandomForestClassifier
from sklearn import svm
import sys

HOME_DIR = r"//bibbity/bobbity/boo/pqi_hospitalizations/ml/"

def gramify(inlist, n = 3):
    # Takes an input list and returns an n-grammed version of that list.
    retlist = inlist
    inlen = len(inlist) - (n - 1)
    for ind in range(0, inlen):
        for g in range(2, n + 1):
            this_gram = "_".join(inlist[ind:ind+g])
            retlist.append(this_gram)
    return retlist

def fetch_data(connection, n = 1000, n_grams = 3):
    # sql_feats = """select top {} f.mrn, c.acg_ip_risk, c.was_hospitalized, concat(feature_type, ':', feature_code) as feature
    #             from lhs_features as f INNER JOIN
    #                  lhs_cohort as c
    #             on   f.mrn = c.mrn
    #             where c.portion = 'development'
    #             order by f.mrn, feature_date, feature_type, feature_code""".format(n)
    sql_feats = """select c.portion, f.mrn, c.acg_ip_risk, c.was_hospitalized, concat(feature_type, ':', feature_code) as feature
                from lhs_features as f INNER JOIN
                     lhs_cohort as c
                on   f.mrn = c.mrn
                order by f.mrn, feature_date, feature_type, feature_code"""

    feats = defaultdict(list)
    ppl   = defaultdict(int)
    acg   = defaultdict(float)
    cur = connection.cursor()
    cur.execute(sql_feats);
    while True:
        row = cur.fetchone()
        if row is None:
            break
        feats[row.mrn].append(row.feature)
        ppl[row.mrn] = row.was_hospitalized, row.portion, row.acg_ip_risk
        acg[row.mrn] = row.acg_ip_risk
    cur.close()

    # for mrn in feats:
    #     feats[mrn] = gramify(feats[mrn], n=n_grams)
        # Add the answer to see that predictions become perfect.
        # feats[mrn].clear()
        # feats[mrn].append(ppl[mrn])

    vec = MultiLabelBinarizer(sparse_output=False) # sparse=True is easier on memory(?) but limits the classifiers you can use
    # xs = vec.fit_transform(feats)  # fit to training data, and convert to 1/0 per feature
    xs = vec.fit_transform(feats.values())  # fit to training data, and convert to 1/0 per feature

    # We have to preserve this or else we can't produce the same prediction matrix for validation.
    joblib.dump(vec, HOME_DIR +"binarizer.pkl")

    # ys = pd.DataFrame(list(ppl.items()), columns=['mrn', 'hospitalized', 'portion'], dtype='int64')
    ys = pd.DataFrame.from_dict(ppl, orient = 'index')
    ys.columns = ['hospitalized', 'portion', 'acg_ip_risk']

    xs = pd.DataFrame(xs)
    xs.index = list(ppl.keys())
    # print(xs.head())
    # print("xs-post")
    # print(xs[1:10])
    acg = pd.DataFrame(list(acg.items()), columns=['mrn', 'acg_ip_risk'])
    acg.set_index('mrn', inplace=True)

    return xs, ys, acg

def print_stats(dat, lab):
    print("{} has {} people in it, {:d} of whom have been hospitalized.".format(lab, dat.shape[0], int(sum(dat.hospitalized))))

def record_predictions(run_id, db, preds):
    print("Recording predictions...")
    for index, row in preds.iterrows():
        for f in row.T.iteritems():
            if f[0] in ['mrn', 'cheating', 'hospitalized', 'portion']:
                pass
            else:
                classifier = f[0].strip()
                # classifier = 'RandmForest'
                sql = "insert into ml_predictions (mrn, run_id, classifier, prediction) values ('{}', {}, '{}', {})".format(row.name, run_id, classifier, row[classifier])
                # sql = "yo."
                # print(sql)
                db.execute(sql)
        db.commit()

def try_pred(run_id, db, clf, y_train, y_test, x_train, x_test):
    # print("Trying {}".format(clf.nickname))
    n_train      = y_train.shape[0]
    n_train_hosp = int(sum(y_train.hospitalized))
    n_test       = y_test.shape[0]
    n_test_hosp  = int(sum(y_test.hospitalized))

    clf.fit(x_train, y_train.hospitalized)

    y_pred = clf.predict(x_test)

    joblib.dump(clf, HOME_DIR +"{}.pkl".format(clf.nickname.strip()))

    ind_preds = None
    if clf.nickname not in ["perceptron ", "LinearSVC"]:
        ind_preds = clf.predict_proba(x_test)[:, 1]

    # how'd we do?
    cm  = confusion_matrix(y_test.hospitalized, y_pred)
    acc = accuracy_score(y_test.hospitalized, y_pred)
    true_neg  = cm[0, 0]
    false_neg = cm[1, 0]
    false_pos = cm[0, 1]
    true_pos  = cm[1, 1]

    recsql = """insert into ml_results(run_id
                                    , classifier
                                    , training_n
                                    , training_hosp
                                    , test_n
                                    , test_hosp
                                    , true_neg
                                    , false_neg
                                    , true_pos
                                    , false_pos
                                    , accuracy)
                            values ({}
                                    ,'{}'
                                    , {}
                                    , {}
                                    , {}
                                    , {}
                                    , {}
                                    , {}
                                    , {}
                                    , {}
                                    , {})""".format(run_id
                                                    , clf.nickname
                                                    , n_train
                                                    , n_train_hosp
                                                    , n_test
                                                    , n_test_hosp
                                                    , true_neg
                                                    , false_neg
                                                    , true_pos
                                                    , false_pos
                                                    , acc)

    db.execute(recsql)
    db.commit()
    print("classifier: {}, true neg:{} false neg:{}, false pos:{}, true pos:{}, accuracy:{}".format(clf.nickname, true_neg, false_neg, false_pos, true_pos, acc))
    return ind_preds

def dev_val_split(xs, ys):
  # input xs is a dataframe indexed on mrn.
  # input ys is a dataframe also indexed on mrn, which contains a column 'portion' giving the split
  val_indexes = ys['portion'] == 'validation'
  return xs[~val_indexes], xs[val_indexes], ys[~val_indexes], ys[val_indexes]

def main(num_iters = 5):
    pyodbc.lowercase = False
    conn = pyodbc.connect(r"DRIVER=SQL Server;Trusted_Connection=Yes;DATABASE=pardee_datascience;SERVER=kpwhri_datascience.ghc.org")
    conn.execute("insert into ml_runs (runtime) values (DEFAULT)")
    conn.commit()
    run_id = conn.execute("select max(run_id) as run_id from ml_runs").fetchone()[0]

    xs, ys, acg_preds = fetch_data(n = 80000, connection = conn)
    # print(acg_preds[0:3])
    # print(len(acg_preds))

    for i in range(num_iters):
        # x_train, x_test, y_train, y_test = train_test_split(xs, ys,
        #                                                     test_size=0.33,
        #                                                     random_state=0,
        #                                                     stratify=ys)
        x_train, x_test, y_train, y_test = dev_val_split(xs, ys)

        preds = y_test.copy(deep=True)
        # preds = pd.DataFrame({'hospitalized': y_test})
        print_stats(y_train, 'Training')
        print_stats(y_test, 'Test')

        clf = Perceptron(max_iter=1000, tol = .001) # TODO: find out what these params do (apart from suppress a warning)
        clf.nickname = "perceptron "
        try_pred(run_id, conn, clf, y_train, y_test, x_train, x_test)

        clf = LinearSVC(random_state=i)
        clf.nickname = "LinearSVC"
        try_pred(run_id, conn, clf, y_train, y_test, x_train, x_test)

        clf = RandomForestClassifier(n_jobs=-1, max_leaf_nodes=100, random_state=i)
        clf.nickname = "RandmForest"
        preds["RandmForest"] = try_pred(run_id, conn, clf, y_train, y_test, x_train, x_test)

        clf = GaussianNB()
        clf.nickname = "naive bayes"
        preds["naive_bayes"] = try_pred(run_id, conn, clf, y_train, y_test, x_train, x_test)

        # clf = LinearDiscriminantAnalysis(solver='svd')
        # clf.nickname = "LDA        "
        # preds["LDA"] = try_pred(run_id, conn, clf, y_train, y_test, x_train, x_test)

        clf = svm.SVC(probability=True)
        clf.nickname = "SVM        "
        preds["SVM"] = try_pred(run_id, conn, clf, y_train, y_test, x_train, x_test)

        preds["Random"] = np.random.rand(preds.shape[0])

        preds = pd.merge(preds, acg_preds, left_index=True, right_index=True)

        preds.to_csv(HOME_DIR + "preds_d.csv")

        record_predictions(run_id, conn, preds)

    conn.close()

if __name__ == '__main__':
    main(num_iters = 1)

print("Finished!")
