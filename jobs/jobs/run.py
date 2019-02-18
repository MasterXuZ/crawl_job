from scrapy import cmdline
import cProfile

# name = 'zhilian'
# cmd = 'scrapy crawl {0}'.format(name)
# cmdline.execute(cmd.split())


# name = 'zhipin'
# cmd = 'scrapy crawl {0}'.format(name)
# # cmdline.execute(cmd.split())
# cProfile.run('cmdline.execute(cmd.split())',sort='time')

name = '51job'
cmd = 'scrapy crawl {0}'.format(name)
cmdline.execute(cmd.split())

