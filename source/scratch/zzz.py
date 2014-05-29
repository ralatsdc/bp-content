# Computes and plots date histogram
            
days = np.array(days)
days = days[days>0]
days = days - min(days)

n_days, edges = np.histogram(days, range(0, 90))
# plt.hist(days, range(0, 90))
plt.step(edges[0:89], np.cumsum(n_days), 'r-')
plt.show()

n_days, edges = np.histogram(days, range(0, 90))

s_days = np.cumsum(n_days)

i_days = min(edges(s_days > 100))

plt.step(edges[0:89], np.cumsum(n_days), 'r-')
plt.step(edges[0:89], n_days, 'b-')

plt.show()
