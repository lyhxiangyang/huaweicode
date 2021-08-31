if __name__ == '__main__':
    # Read selected features of wrf and grapes
    f = open('hjx/Features/wrf.txt', 'r')
    wrf_features = f.read().splitlines()
    print('Number of features of wrf:', len(wrf_features))

    f = open('hjx/Features/grapes.txt', 'r')
    grapes_features = f.read().splitlines()
    print('Number of features of grapes:', len(grapes_features))

    # Calculate the intersection
    intersection = [i for i in wrf_features if i in grapes_features]
    print('Size of intersection:', len(intersection))

    # Calculate and save the difference
    diff_in_wrf = [i for i in wrf_features if i not in grapes_features]
    print(len(diff_in_wrf))
    f = open('hjx/Features/diff_in_wrf.txt', 'w')
    f.write('\n'.join(diff_in_wrf))
    f.close()

    diff_in_grapes = [i for i in grapes_features if i not in wrf_features]
    print(len(diff_in_grapes))
    f = open('hjx/Features/diff_in_grapes.txt', 'w')
    f.write('\n'.join(diff_in_grapes))
    f.close()
